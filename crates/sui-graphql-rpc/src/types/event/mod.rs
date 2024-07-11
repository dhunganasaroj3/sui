// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use super::cursor::{self, Page, Paginated, ScanLimited, Target};
use super::digest::Digest;
use super::type_filter::{ModuleFilter, TypeFilter};
use super::{
    address::Address, base64::Base64, date_time::DateTime, move_module::MoveModule,
    move_value::MoveValue, sui_address::SuiAddress,
};
use crate::data::{self, QueryExecutor};
use crate::{data::Db, error::Error};
use async_graphql::connection::{Connection, CursorType, Edge};
use async_graphql::*;
use diesel::{ExpressionMethods, NullableExpressionMethods, QueryDsl};
use sui_indexer::models::{events::StoredEvent, transactions::StoredTransaction};
use sui_indexer::schema::{events, transactions, tx_senders};
use sui_types::base_types::ObjectID;
use sui_types::Identifier;
use sui_types::{
    base_types::SuiAddress as NativeSuiAddress, event::Event as NativeEvent, parse_sui_struct_tag,
};

pub(crate) use ev_cursor::Cursor;

mod ev_cursor;

/// A Sui node emits one of the following events:
/// Move event
/// Publish event
/// Transfer object event
/// Delete object event
/// New object event
/// Epoch change event
#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub stored: Option<StoredEvent>,
    pub native: NativeEvent,
    /// The checkpoint sequence number this was viewed at.
    pub checkpoint_viewed_at: u64,
}

type Query<ST, GB> = data::Query<ST, events::table, GB>;

#[derive(InputObject, Clone, Default)]
pub(crate) struct EventFilter {
    pub sender: Option<SuiAddress>,
    pub transaction_digest: Option<Digest>,
    // Enhancement (post-MVP)
    // after_checkpoint
    // before_checkpoint
    /// Events emitted by a particular module. An event is emitted by a
    /// particular module if some function in the module is called by a
    /// PTB and emits an event.
    ///
    /// Modules can be filtered by their package, or package::module.
    pub emitting_module: Option<ModuleFilter>,

    /// This field is used to specify the type of event emitted.
    ///
    /// Events can be filtered by their type's package, package::module,
    /// or their fully qualified type name.
    ///
    /// Generic types can be queried by either the generic type name, e.g.
    /// `0x2::coin::Coin`, or by the full type name, such as
    /// `0x2::coin::Coin<0x2::sui::SUI>`.
    pub event_type: Option<TypeFilter>,
    // Enhancement (post-MVP)
    // pub start_time
    // pub end_time

    // Enhancement (post-MVP)
    // pub any
    // pub all
    // pub not
}

#[Object]
impl Event {
    /// The Move module containing some function that when called by
    /// a programmable transaction block (PTB) emitted this event.
    /// For example, if a PTB invokes A::m1::foo, which internally
    /// calls A::m2::emit_event to emit an event,
    /// the sending module would be A::m1.
    async fn sending_module(&self, ctx: &Context<'_>) -> Result<Option<MoveModule>> {
        MoveModule::query(
            ctx,
            self.native.package_id.into(),
            &self.native.transaction_module.to_string(),
            self.checkpoint_viewed_at,
        )
        .await
        .extend()
    }

    /// Address of the sender of the event
    async fn sender(&self) -> Result<Option<Address>> {
        if self.native.sender == NativeSuiAddress::ZERO {
            return Ok(None);
        }

        Ok(Some(Address {
            address: self.native.sender.into(),
            checkpoint_viewed_at: self.checkpoint_viewed_at,
        }))
    }

    /// UTC timestamp in milliseconds since epoch (1/1/1970)
    async fn timestamp(&self) -> Result<Option<DateTime>, Error> {
        if let Some(stored) = &self.stored {
            Ok(Some(DateTime::from_ms(stored.timestamp_ms)?))
        } else {
            Ok(None)
        }
    }

    #[graphql(flatten)]
    async fn move_value(&self) -> Result<MoveValue> {
        Ok(MoveValue::new(
            self.native.type_.clone().into(),
            Base64::from(self.native.contents.clone()),
        ))
    }
}

impl Event {
    /// Query the database for a `page` of events. The Page uses the transaction, event, and
    /// checkpoint sequence numbers as the cursor to determine the correct page of results. The
    /// query can optionally be further `filter`-ed by the `EventFilter`.
    ///
    /// The `checkpoint_viewed_at` parameter is represents the checkpoint sequence number at which
    /// this page was queried for. Each entity returned in the connection will inherit this
    /// checkpoint, so that when viewing that entity's state, it will be from the reference of this
    /// checkpoint_viewed_at parameter.
    ///
    /// If the `Page<Cursor>` is set, then this function will defer to the `checkpoint_viewed_at` in
    /// the cursor if they are consistent.
    pub(crate) async fn paginate(
        db: &Db,
        page: Page<Cursor>,
        filter: EventFilter,
        checkpoint_viewed_at: u64,
    ) -> Result<Connection<String, Event>, Error> {
        let cursor_viewed_at = page.validate_cursor_consistency()?;
        let checkpoint_viewed_at = cursor_viewed_at.unwrap_or(checkpoint_viewed_at);

        let mut ctes = vec![];

        if let Some(digest) = &filter.transaction_digest {
            ctes.push(format!(
                r#"
                tx_eq AS (
                    SELECT tx_sequence_number AS eq
                    FROM tx_digests
                    WHERE digest = {}
                )
            "#,
                bytea_literal(digest)
            ));
        }
        if let Some(after) = page.after() {
            ctes.push(format!(
                r#"
                tx_lo AS (
                    SELECT GREATEST({}, (SELECT eq FROM tx_eq)) AS lo
                ),
                ev_lo AS (
                    SELECT CASE
                        WHEN (SELECT lo FROM tx_lo) > {} THEN {} ELSE {}
                    END AS lo
                )
            "#,
                after.tx, after.tx, 0, after.e,
            ));
        }

        /**
                         * if page.after {
                         * tx_lo AS (SELECT GREATEST(?, (SELECT eq from tx_eq)) AS lo)
                         * ev_lo AS (SELECT CASE
                         *  WHEN (SELECT lo FROM tx_lo) > ? THEN ? ELSE ?
                         * END as lo)
                         * }
                         *
                         * if page.before {
                         * tx_hi AS (
                    SELECT LEAST(?, (SELECT eq FROM tx_eq)) AS hi
                ),

                ev_hi AS (
                    SELECT CASE
                        WHEN (SELECT hi FROM tx_hi) < ? THEN ?
                        ELSE ?
                    END AS hi
                ) }
                         *
                         *
                        sdlfkjsdflkjlfkjsldfkjsdlfkkkk*
                         *
                         * WITH tx_eq AS (
            SELECT tx_sequence_number AS eq
            FROM amnn_0_hybrid_tx_digests
            WHERE tx_digest = ?
        ),
        tx_hi AS (
            SELECT LEAST(?, (SELECT eq FROM tx_eq)) AS hi
        ),
        ev_hi AS (
            SELECT CASE
                WHEN (SELECT hi FROM tx_hi) < ? THEN ?
                ELSE ?
            END AS hi
        )
        SELECT tx_sequence_number, event_sequence_number
        FROM amnn_0_hybrid_ev_event_mod
        WHERE ((package = ?) AND (module = ?))
            AND (sender = ?)
            AND (tx_sequence_number >= (SELECT lo FROM tx_lo))
            AND (tx_sequence_number <= (SELECT hi FROM tx_hi))
            AND (ROW(tx_sequence_number, event_sequence_number) >= ((SELECT lo FROM tx_lo), (SELECT lo FROM ev_lo)))
            AND (ROW(tx_sequence_number, event_sequence_number) <= ((SELECT hi FROM tx_hi), (SELECT hi FROM ev_hi)))
        ORDER BY tx_sequence_number ASC, event_sequence_number ASC
        LIMIT ?
                         */
        // ok so we have
        // sender
        // digest
        // emitting
        // event

        // combinations with sender and digest are fine
        // basically can't combine emitting and event

        // if we make two roundtrips then we can continue using query dsl
        // otherwise we'll have to follow a similar format to what we did to tx...

        // apply, redundantly, tlo and thi

        // emitting -> event_emit_package, event_emit_module
        // event_type -> event_struct_package, _module, _name, _instantiation?

        // em-pkg (conj [:= :package (hex->bytes em-pkg)])
        // em-mod (conj [:= :module  em-mod])

        // ev-pkg (conj [:= :event-type-package (hex->bytes ev-pkg)])
        // ev-mod (conj [:= :event-type-module  ev-mod])
        // ev-typ (conj [:= :event-type-name (first (s/split ev-typ #"<" 2))])

        // (and ev-typ (s/includes? ev-typ "<"))
        // (conj [:= :event-type (str (hex/normalize ev-pkg)
        //    "::" ev-mod "::" ev-typ)])
        let (prev, next, results) = db
            .execute(move |conn| {
                page.paginate_query::<StoredEvent, _, _, _>(conn, checkpoint_viewed_at, move || {
                    let mut query = events::dsl::events.into_boxed();

                    // Bound events by the provided `checkpoint_viewed_at`. From EXPLAIN
                    // ANALYZE, using the checkpoint sequence number directly instead of
                    // translating into a transaction sequence number bound is more efficient.
                    query = query.filter(
                        events::dsl::checkpoint_sequence_number.le(checkpoint_viewed_at as i64),
                    );

                    // The transactions table doesn't have an index on the senders column, so use
                    // `tx_senders`.
                    if let Some(sender) = &filter.sender {
                        query = query.filter(
                            events::dsl::tx_sequence_number.eq_any(
                                tx_senders::dsl::tx_senders
                                    .select(tx_senders::dsl::tx_sequence_number)
                                    .filter(tx_senders::dsl::sender.eq(sender.into_vec())),
                            ),
                        )
                    }

                    if let Some(digest) = &filter.transaction_digest {
                        // Since the event filter takes in a single tx_digest, we know that
                        // there will only be one corresponding transaction. We can use
                        // single_value() to tell the query planner that we expect only one
                        // instead of a range of values, which will subsequently speed up query
                        // execution time.
                        query = query.filter(
                            events::dsl::tx_sequence_number.nullable().eq(
                                transactions::dsl::transactions
                                    .select(transactions::dsl::tx_sequence_number)
                                    .filter(
                                        transactions::dsl::transaction_digest.eq(digest.to_vec()),
                                    )
                                    .single_value(),
                            ),
                        )
                    }

                    if let Some(module) = &filter.emitting_module {
                        query = module.apply(query, events::dsl::package, events::dsl::module);
                    }

                    if let Some(type_) = &filter.event_type {
                        query = type_.apply(
                            query,
                            events::dsl::event_type,
                            events::dsl::event_type_package,
                            events::dsl::event_type_module,
                            events::dsl::event_type_name,
                        );
                    }

                    query
                })
            })
            .await?;

        let mut conn = Connection::new(prev, next);

        // The "checkpoint viewed at" sets a consistent upper bound for the nested queries.
        for stored in results {
            let cursor = stored.cursor(checkpoint_viewed_at).encode_cursor();
            conn.edges.push(Edge::new(
                cursor,
                Event::try_from_stored_event(stored, checkpoint_viewed_at)?,
            ));
        }

        Ok(conn)
    }

    pub(crate) fn try_from_stored_transaction(
        stored_tx: &StoredTransaction,
        idx: usize,
        checkpoint_viewed_at: u64,
    ) -> Result<Self, Error> {
        let Some(serialized_event) = &stored_tx.get_event_at_idx(idx) else {
            return Err(Error::Internal(format!(
                "Could not find event with event_sequence_number {} at transaction {}",
                idx, stored_tx.tx_sequence_number
            )));
        };

        let native_event: NativeEvent = bcs::from_bytes(serialized_event).map_err(|_| {
            Error::Internal(format!(
                "Failed to deserialize event with {} at transaction {}",
                idx, stored_tx.tx_sequence_number
            ))
        })?;

        let stored_event = StoredEvent {
            tx_sequence_number: stored_tx.tx_sequence_number,
            event_sequence_number: idx as i64,
            transaction_digest: stored_tx.transaction_digest.clone(),
            checkpoint_sequence_number: stored_tx.checkpoint_sequence_number,
            #[cfg(feature = "postgres-feature")]
            senders: vec![Some(native_event.sender.to_vec())],
            #[cfg(feature = "mysql-feature")]
            #[cfg(not(feature = "postgres-feature"))]
            senders: serde_json::to_value(vec![native_event.sender.to_vec()]).unwrap(),
            package: native_event.package_id.to_vec(),
            module: native_event.transaction_module.to_string(),
            event_type: native_event
                .type_
                .to_canonical_string(/* with_prefix */ true),
            event_type_package: native_event.type_.address.to_vec(),
            event_type_module: native_event.type_.module.to_string(),
            event_type_name: native_event.type_.name.to_string(),
            bcs: native_event.contents.clone(),
            timestamp_ms: stored_tx.timestamp_ms,
        };

        Ok(Self {
            stored: Some(stored_event),
            native: native_event,
            checkpoint_viewed_at,
        })
    }

    fn try_from_stored_event(
        stored: StoredEvent,
        checkpoint_viewed_at: u64,
    ) -> Result<Self, Error> {
        let Some(Some(sender_bytes)) = ({
            #[cfg(feature = "postgres-feature")]
            {
                stored.senders.first()
            }
            #[cfg(feature = "mysql-feature")]
            #[cfg(not(feature = "postgres-feature"))]
            {
                stored
                    .senders
                    .as_array()
                    .ok_or_else(|| {
                        Error::Internal("Failed to parse event senders as array".to_string())
                    })?
                    .first()
            }
        }) else {
            return Err(Error::Internal("No senders found for event".to_string()));
        };
        let sender = NativeSuiAddress::from_bytes(sender_bytes)
            .map_err(|e| Error::Internal(e.to_string()))?;
        let package_id =
            ObjectID::from_bytes(&stored.package).map_err(|e| Error::Internal(e.to_string()))?;
        let type_ =
            parse_sui_struct_tag(&stored.event_type).map_err(|e| Error::Internal(e.to_string()))?;
        let transaction_module =
            Identifier::from_str(&stored.module).map_err(|e| Error::Internal(e.to_string()))?;
        let contents = stored.bcs.clone();
        Ok(Event {
            stored: Some(stored),
            native: NativeEvent {
                sender,
                package_id,
                transaction_module,
                type_,
                contents,
            },
            checkpoint_viewed_at,
        })
    }
}
