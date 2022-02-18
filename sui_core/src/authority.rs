// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use move_bytecode_utils::module_cache::ModuleCache;
use move_core_types::{
    account_address::AccountAddress,
    language_storage::{ModuleId, StructTag},
    resolver::{ModuleResolver, ResourceResolver},
};
use move_vm_runtime::native_functions::NativeFunctionTable;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    pin::Pin,
    sync::Arc,
};
use sui_adapter::{adapter, genesis};
use sui_types::{
    base_types::*,
    committee::Committee,
    error::{SuiError, SuiResult},
    fp_bail, fp_ensure, gas,
    messages::*,
    object::{Data, Object},
    storage::Storage,
};

#[cfg(test)]
#[path = "unit_tests/authority_tests.rs"]
mod authority_tests;

mod temporary_store;
use temporary_store::AuthorityTemporaryStore;

mod authority_store;
pub use authority_store::AuthorityStore;

/// a Trait object for `signature::Signer` that is:
/// - Pin, i.e. confined to one place in memory (we don't want to copy private keys).
/// - Sync, i.e. can be safely shared between threads.
///
/// Typically instantiated with Box::pin(keypair) where keypair is a `KeyPair`
///
type StableSyncSigner = Pin<Box<dyn signature::Signer<ed25519_dalek::Signature> + Send + Sync>>;

pub struct AuthorityState {
    // Fixed size, static, identity of the authority
    /// The name of this authority.
    pub name: AuthorityName,
    /// Committee of this Sui instance.
    pub committee: Committee,
    /// The signature key of the authority.
    pub secret: StableSyncSigner,

    /// Move native functions that are available to invoke
    _native_functions: NativeFunctionTable,
    move_vm: Arc<adapter::MoveVM>,

    /// The database
    _database: Arc<AuthorityStore>,
}

/// The authority state encapsulates all state, drives execution, and ensures safety.
///
/// Note the authority operations can be accesessed through a read ref (&) and do not
/// require &mut. Internally a database is syncronized through a mutex lock.
///
/// Repeating valid commands should produce no changes and return no error.
impl AuthorityState {
    /// The logic to check one object against a reference, and return the object if all is well
    /// or an error if not.
    fn check_one_lock(
        &self,
        order: &Order,
        object_kind: InputObjectKind,
        object: &Object,
        mutable_object_addresses: &HashSet<SuiAddress>,
    ) -> SuiResult {
        match object_kind {
            InputObjectKind::MovePackage(package_id) => {
                fp_ensure!(
                    object.data.try_as_package().is_some(),
                    SuiError::MoveObjectAsPackage {
                        object_id: package_id
                    }
                );
            }
            InputObjectKind::MoveObject((object_id, sequence_number, object_digest)) => {
                fp_ensure!(
                    sequence_number <= SequenceNumber::max(),
                    SuiError::InvalidSequenceNumber
                );

                // Check that the seq number is the same
                fp_ensure!(
                    object.version() == sequence_number,
                    SuiError::UnexpectedSequenceNumber {
                        object_id,
                        expected_sequence: object.version(),
                    }
                );

                // Check the digest matches
                fp_ensure!(
                    object.digest() == object_digest,
                    SuiError::InvalidObjectDigest {
                        object_id,
                        expected_digest: object_digest
                    }
                );

                if object.is_read_only() {
                    // Gas object must not be immutable.
                    fp_ensure!(
                        object_id != order.gas_payment_object_ref().0,
                        SuiError::InsufficientGas {
                            error: "Gas object should not be immutable".to_string()
                        }
                    );
                    // Checks for read-only objects end here.
                    return Ok(());
                }

                // Additional checks for mutable objects
                // Check the object owner is either the transaction sender, or
                // another mutable object in the input.
                fp_ensure!(
                    order.sender_address() == object.owner
                        || mutable_object_addresses.contains(&object.owner),
                    SuiError::IncorrectSigner
                );

                if object_id == order.gas_payment_object_ref().0 {
                    gas::check_gas_requirement(order, object)?;
                }
            }
        };
        Ok(())
    }

    /// Check all the objects used in the order against the database, and ensure
    /// that they are all the correct version and number.
    async fn check_locks(&self, order: &Order) -> Result<Vec<(InputObjectKind, Object)>, SuiError> {
        let input_objects = order.input_objects();
        let mut all_objects = Vec::with_capacity(input_objects.len());

        // There is at least one input
        fp_ensure!(
            !input_objects.is_empty(),
            SuiError::ObjectInputArityViolation
        );
        // Ensure that there are no duplicate inputs
        let mut used = HashSet::new();
        if !input_objects.iter().all(|o| used.insert(o.object_id())) {
            return Err(SuiError::DuplicateObjectRefInput);
        }

        let ids: Vec<_> = input_objects.iter().map(|kind| kind.object_id()).collect();

        let objects = self.get_objects(&ids[..]).await?;
        let mutable_object_addresses: HashSet<_> = objects
            .iter()
            .flat_map(|opt_obj| match opt_obj {
                Some(obj) if !obj.is_read_only() => Some(obj.id().into()),
                _ => None,
            })
            .collect();
        let mut errors = Vec::new();
        for (object_kind, object) in input_objects.into_iter().zip(objects) {
            let object = match object {
                Some(object) => object,
                None => {
                    errors.push(object_kind.object_not_found_error());
                    continue;
                }
            };

            match self.check_one_lock(order, object_kind, &object, &mutable_object_addresses) {
                Ok(()) => all_objects.push((object_kind, object)),
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        // If any errors with the locks were detected, we return all errors to give the client
        // a chance to update the authority if possible.
        if !errors.is_empty() {
            return Err(SuiError::LockErrors { errors });
        }

        fp_ensure!(!all_objects.is_empty(), SuiError::ObjectInputArityViolation);

        Ok(all_objects)
    }

    /// Initiate a new order.
    pub async fn handle_order(&self, order: Order) -> Result<OrderInfoResponse, SuiError> {
        // Check the sender's signature.
        order.check_signature()?;
        let transaction_digest = order.digest();

        let mutable_objects: Vec<_> = self
            .check_locks(&order)
            .await?
            .into_iter()
            .filter_map(|(object_kind, object)| match object_kind {
                InputObjectKind::MovePackage(_) => None,
                InputObjectKind::MoveObject(object_ref) => {
                    if object.is_read_only() {
                        None
                    } else {
                        Some(object_ref)
                    }
                }
            })
            .collect();

        let signed_order = SignedOrder::new(order, self.name, &*self.secret);

        // Check and write locks, to signed order, into the database
        // The call to self.set_order_lock checks the lock is not conflicting,
        // and returns ConflictingOrder error in case there is a lock on a different
        // existing order.
        self.set_order_lock(&mutable_objects, signed_order).await?;

        // Return the signed Order or maybe a cert.
        self.make_order_info(&transaction_digest).await
    }

    /// Confirm a transfer.
    pub async fn handle_confirmation_order(
        &self,
        confirmation_order: ConfirmationOrder,
    ) -> Result<OrderInfoResponse, SuiError> {
        let certificate = confirmation_order.certificate;
        let order = certificate.order.clone();
        let transaction_digest = order.digest();

        // Check the certificate and retrieve the transfer data.
        certificate.check(&self.committee)?;

        // Ensure an idempotent answer
        let order_info = self.make_order_info(&transaction_digest).await?;
        if order_info.certified_order.is_some() {
            return Ok(order_info);
        }

        let inputs: Vec<_> = self
            .check_locks(&order)
            .await?
            .into_iter()
            .map(|(_, object)| object)
            .collect();

        let mut transaction_dependencies: BTreeSet<_> = inputs
            .iter()
            .map(|object| object.previous_transaction)
            .collect();

        // Insert into the certificates map
        let mut tx_ctx = TxContext::new(&order.sender_address(), transaction_digest);

        let gas_object_id = order.gas_payment_object_ref().0;
        let (mut temporary_store, status) = self.execute_order(order, inputs, &mut tx_ctx)?;

        // Remove from dependencies the generic hash
        transaction_dependencies.remove(&TransactionDigest::genesis());

        let unwrapped_object_ids = self.get_unwrapped_object_ids(temporary_store.written())?;
        temporary_store.patch_unwrapped_object_version(unwrapped_object_ids);
        let to_signed_effects = temporary_store.to_signed_effects(
            &self.name,
            &*self.secret,
            &transaction_digest,
            transaction_dependencies.into_iter().collect(),
            status,
            &gas_object_id,
        );
        // Update the database in an atomic manner
        self.update_state(temporary_store, certificate, to_signed_effects)
            .await // Returns the OrderInfoResponse
    }

    fn execute_order(
        &self,
        order: Order,
        mut inputs: Vec<Object>,
        tx_ctx: &mut TxContext,
    ) -> SuiResult<(AuthorityTemporaryStore, ExecutionStatus)> {
        let mut temporary_store = AuthorityTemporaryStore::new(self, &inputs, tx_ctx.digest());
        // unwraps here are safe because we built `inputs`
        let mut gas_object = inputs.pop().unwrap();

        let sender = order.sender_address();
        let status = match order.data.kind {
            OrderKind::Transfer(t) => AuthorityState::transfer(
                &mut temporary_store,
                inputs,
                t.recipient,
                gas_object.clone(),
            ),
            OrderKind::Call(c) => {
                // unwraps here are safe because we built `inputs`
                let package = inputs.pop().unwrap();
                adapter::execute(
                    &self.move_vm,
                    &mut temporary_store,
                    self._native_functions.clone(),
                    package,
                    &c.module,
                    &c.function,
                    c.type_arguments,
                    inputs,
                    c.pure_arguments,
                    c.gas_budget,
                    gas_object.clone(),
                    tx_ctx,
                )
            }
            OrderKind::Publish(m) => adapter::publish(
                &mut temporary_store,
                self._native_functions.clone(),
                m.modules,
                sender,
                tx_ctx,
                m.gas_budget,
                gas_object.clone(),
            ),
        }?;
        if let ExecutionStatus::Failure { gas_used, .. } = &status {
            // Roll back the temporary store if execution failed.
            temporary_store.reset();
            // This gas deduction cannot fail.
            gas::deduct_gas(&mut gas_object, *gas_used);
            temporary_store.write_object(gas_object);
        }
        temporary_store.ensure_active_inputs_mutated();
        Ok((temporary_store, status))
    }

    fn transfer(
        temporary_store: &mut AuthorityTemporaryStore,
        mut inputs: Vec<Object>,
        recipient: SuiAddress,
        mut gas_object: Object,
    ) -> SuiResult<ExecutionStatus> {
        if !inputs.len() == 1 {
            return Ok(ExecutionStatus::Failure {
                gas_used: gas::MIN_OBJ_TRANSFER_GAS,
                error: Box::new(SuiError::ObjectInputArityViolation),
            });
        }

        // Safe to do pop due to check !is_empty()
        let mut output_object = inputs.pop().unwrap();

        let gas_used = gas::calculate_object_transfer_cost(&output_object);
        if let Err(err) = gas::try_deduct_gas(&mut gas_object, gas_used) {
            return Ok(ExecutionStatus::Failure {
                gas_used: gas::MIN_OBJ_TRANSFER_GAS,
                error: Box::new(err),
            });
        }
        temporary_store.write_object(gas_object);

        if output_object.is_read_only() {
            return Ok(ExecutionStatus::Failure {
                gas_used: gas::MIN_OBJ_TRANSFER_GAS,
                error: Box::new(SuiError::CannotTransferReadOnlyObject),
            });
        }

        output_object.transfer(recipient);
        temporary_store.write_object(output_object);
        Ok(ExecutionStatus::Success { gas_used })
    }

    pub async fn handle_order_info_request(
        &self,
        request: OrderInfoRequest,
    ) -> Result<OrderInfoResponse, SuiError> {
        self.make_order_info(&request.transaction_digest).await
    }

    pub async fn handle_account_info_request(
        &self,
        request: AccountInfoRequest,
    ) -> Result<AccountInfoResponse, SuiError> {
        self.make_account_info(request.account)
    }

    pub async fn handle_object_info_request(
        &self,
        request: ObjectInfoRequest,
    ) -> Result<ObjectInfoResponse, SuiError> {
        // Only add a certificate if it is requested
        let ref_and_digest = if let Some(seq) = request.request_sequence_number {
            // Get the Transaction Digest that created the object
            let parent_iterator = self
                .get_parent_iterator(request.object_id, Some(seq))
                .await?;

            parent_iterator
                .first()
                .map(|(object_ref, tx_digest)| (*object_ref, *tx_digest))
        } else {
            // Or get the latest object_reference and transaction entry.
            self.get_latest_parent_entry(request.object_id).await?
        };

        let (requested_object_reference, parent_certificate) = match ref_and_digest {
            Some((object_ref, transaction_digest)) => (
                Some(object_ref),
                if transaction_digest == TransactionDigest::genesis() {
                    None
                } else {
                    // Get the cert from the transaction digest
                    Some(self.read_certificate(&transaction_digest).await?.ok_or(
                        SuiError::CertificateNotfound {
                            certificate_digest: transaction_digest,
                        },
                    )?)
                },
            ),
            None => (None, None),
        };

        // Always attempt to return the latest version of the object and the
        // current lock if any.
        let object_result = self.get_object(&request.object_id).await;
        let object_and_lock = match object_result {
            Ok(Some(object)) => {
                let lock = if object.is_read_only() {
                    // Read only objects have no locks.
                    None
                } else {
                    self.get_order_lock(&object.to_object_reference()).await?
                };
                let layout = match request.request_layout {
                    Some(format) => {
                        let resolver = ModuleCache::new(&self);
                        object.get_layout(format, &resolver)?
                    }
                    None => None,
                };

                Some(ObjectResponse {
                    object,
                    lock,
                    layout,
                })
            }
            Err(e) => return Err(e),
            _ => None,
        };

        Ok(ObjectInfoResponse {
            parent_certificate,
            requested_object_reference,
            object_and_lock,
        })
    }

    pub async fn new_with_genesis_modules(
        committee: Committee,
        name: AuthorityName,
        secret: StableSyncSigner,
        store: Arc<AuthorityStore>,
    ) -> Self {
        let (genesis_modules, native_functions) = genesis::clone_genesis_data();
        let state = AuthorityState {
            committee,
            name,
            secret,
            _native_functions: native_functions.clone(),
            move_vm: adapter::new_move_vm(native_functions)
                .expect("We defined natives to not fail here"),
            _database: store,
        };

        for genesis_module in genesis_modules {
            #[cfg(debug_assertions)]
            genesis_module.data.try_as_package().unwrap();

            state
                .init_order_lock(genesis_module.to_object_reference())
                .await;
            state.insert_object(genesis_module).await;
        }
        state
    }

    #[cfg(test)]
    pub fn new_without_genesis_for_testing(
        committee: Committee,
        name: AuthorityName,
        secret: StableSyncSigner,
        store: Arc<AuthorityStore>,
    ) -> Self {
        let native_functions = NativeFunctionTable::new();
        AuthorityState {
            committee,
            name,
            secret,
            _native_functions: native_functions.clone(),
            move_vm: adapter::new_move_vm(native_functions).expect("Only fails due to natives."),
            _database: store,
        }
    }

    async fn get_object(&self, object_id: &ObjectID) -> Result<Option<Object>, SuiError> {
        self._database.get_object(object_id)
    }

    pub async fn insert_object(&self, object: Object) {
        self._database
            .insert_object(object)
            .expect("TODO: propagate the error")
    }

    /// Make an information response for an order
    async fn make_order_info(
        &self,
        transaction_digest: &TransactionDigest,
    ) -> Result<OrderInfoResponse, SuiError> {
        self._database.get_order_info(transaction_digest)
    }

    fn make_account_info(&self, account: SuiAddress) -> Result<AccountInfoResponse, SuiError> {
        self._database
            .get_account_objects(account)
            .map(|object_ids| AccountInfoResponse {
                object_ids,
                owner: account,
            })
    }

    // Helper function to manage order_locks

    /// Initialize an order lock for an object/sequence to None
    pub async fn init_order_lock(&self, object_ref: ObjectRef) {
        self._database
            .init_order_lock(object_ref)
            .expect("TODO: propagate the error")
    }

    /// Set the order lock to a specific transaction
    pub async fn set_order_lock(
        &self,
        mutable_input_objects: &[ObjectRef],
        signed_order: SignedOrder,
    ) -> Result<(), SuiError> {
        self._database
            .set_order_lock(mutable_input_objects, signed_order)
    }

    async fn update_state(
        &self,
        temporary_store: AuthorityTemporaryStore,
        certificate: CertifiedOrder,
        signed_effects: SignedOrderEffects,
    ) -> Result<OrderInfoResponse, SuiError> {
        self._database
            .update_state(temporary_store, certificate, signed_effects)
    }

    /// Get a read reference to an object/seq lock
    pub async fn get_order_lock(
        &self,
        object_ref: &ObjectRef,
    ) -> Result<Option<SignedOrder>, SuiError> {
        self._database.get_order_lock(object_ref)
    }

    // Helper functions to manage certificates

    /// Read from the DB of certificates
    pub async fn read_certificate(
        &self,
        digest: &TransactionDigest,
    ) -> Result<Option<CertifiedOrder>, SuiError> {
        self._database.read_certificate(digest)
    }

    pub async fn parent(&self, object_ref: &ObjectRef) -> Option<TransactionDigest> {
        self._database
            .parent(object_ref)
            .expect("TODO: propagate the error")
    }

    pub async fn get_objects(
        &self,
        _objects: &[ObjectID],
    ) -> Result<Vec<Option<Object>>, SuiError> {
        self._database.get_objects(_objects)
    }

    /// Returns all parents (object_ref and transaction digests) that match an object_id, at
    /// any object version, or optionally at a specific version.
    pub async fn get_parent_iterator(
        &self,
        object_id: ObjectID,
        seq: Option<SequenceNumber>,
    ) -> Result<Vec<(ObjectRef, TransactionDigest)>, SuiError> {
        {
            self._database.get_parent_iterator(object_id, seq)
        }
    }

    pub async fn get_latest_parent_entry(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectRef, TransactionDigest)>, SuiError> {
        self._database.get_latest_parent_entry(object_id)
    }

    /// Given all mutated objects during a transaction, return the list of objects
    /// that were unwrapped (i.e. re-appeared after being deleted).
    fn get_unwrapped_object_ids(
        &self,
        written: &BTreeMap<ObjectID, Object>,
    ) -> Result<Vec<ObjectID>, SuiError> {
        // For each mutated object, we first find out whether there was a transaction
        // that deleted this object in the past.
        let parents = self._database.multi_get_parents(
            &written
                .iter()
                .map(|(object_id, object)| (*object_id, object.version(), OBJECT_DIGEST_DELETED))
                .collect::<Vec<_>>(),
        )?;
        // Filter the list of mutated objects based on whether they were deleted in the past.
        // These objects are the unwrapped ones.
        let filtered = written
            .iter()
            .zip(parents.iter())
            .filter_map(|((object_id, _object), d)| d.map(|_| *object_id))
            .collect();
        Ok(filtered)
    }
}

impl ModuleResolver for AuthorityState {
    type Error = SuiError;

    fn get_module(&self, module_id: &ModuleId) -> Result<Option<Vec<u8>>, Self::Error> {
        self._database.get_module(module_id)
    }
}
