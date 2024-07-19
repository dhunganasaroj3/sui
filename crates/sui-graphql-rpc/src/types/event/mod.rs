// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use super::cursor::{Page, Target};
use super::digest::Digest;
use super::type_filter::{ModuleFilter, TypeFilter};
use super::{
    address::Address, base64::Base64, date_time::DateTime, move_module::MoveModule,
    move_value::MoveValue, sui_address::SuiAddress,
};
use crate::data::pg::bytea_literal;
use crate::data::{self, DbConnection, QueryExecutor};
use crate::raw_query::RawQuery;
use crate::{data::Db, error::Error};
use crate::{filter, query};
use async_graphql::connection::{Connection, CursorType, Edge};
use async_graphql::*;
use cursor::EvLookup;
use sui_indexer::models::{events::StoredEvent, transactions::StoredTransaction};
use sui_indexer::schema::events;
use sui_types::base_types::ObjectID;
use sui_types::Identifier;
use sui_types::{
    base_types::SuiAddress as NativeSuiAddress, event::Event as NativeEvent, parse_sui_struct_tag,
};

mod cursor;
pub(crate) use cursor::Cursor;

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

        let mut query = match (&filter.sender, &filter.emitting_module, &filter.event_type) {
            (None, None, None) => {
                query!("SELECT * FROM events")
            }
            (Some(sender), None, None) => {
                filter!(
                    query!("SELECT tx_sequence_number, event_sequence_number FROM event_senders"),
                    format!("sender = {}", bytea_literal(sender.as_slice()))
                )
            }
            (sender, None, Some(event_type)) => {
                let query = match event_type {
                    TypeFilter::ByModule(ModuleFilter::ByPackage(p)) => {
                        filter!(
                            query!(
                            "SELECT tx_sequence_number, event_sequence_number FROM event_struct_package"),
                            format!("package = {}", bytea_literal(p.as_slice()))
                        )
                    }
                    TypeFilter::ByModule(ModuleFilter::ByModule(p, m)) => {
                        filter!(
                            query!(
                                "SELECT tx_sequence_number, event_sequence_number FROM event_struct_module"
                            ),
                            format!(
                                "package = {} and module = {{}}",
                                bytea_literal(p.as_slice())
                            ),
                            m
                        )
                    }
                    TypeFilter::ByType(tag) => {
                        let p = tag.address.to_vec();
                        let m = tag.module.to_string();
                        let exact = tag.to_canonical_string(/* with_prefix */ true);
                        let t = exact.split("::").nth(2).unwrap();

                        let (table, col_name) = if tag.type_params.is_empty() {
                            ("event_struct_name", "type_name")
                        } else {
                            ("event_struct_name_instantiation", "type_instantiation")
                        };

                        filter!(
                            query!(format!(
                                "SELECT tx_sequence_number, event_sequence_number FROM {}",
                                table
                            )),
                            format!(
                                "package = {} and module = {{}} and {} = {{}}",
                                bytea_literal(p.as_slice()),
                                col_name
                            ),
                            m,
                            t
                        )
                    }
                };

                if let Some(sender) = sender {
                    filter!(
                        query,
                        format!("sender = {}", bytea_literal(sender.as_slice()))
                    )
                } else {
                    query
                }
            }

            (sender, Some(module), None) => {
                let query = {
                    match module {
                        ModuleFilter::ByPackage(p) => {
                            filter!(
                                query!(
                                "SELECT tx_sequence_number, event_sequence_number FROM event_emit_package"
                            ),
                                format!("package = {}", bytea_literal(p.as_slice()))
                            )
                        }
                        ModuleFilter::ByModule(p, m) => {
                            filter!(
                                query!(
                                    "SELECT tx_sequence_number, event_sequence_number FROM event_emit_module"
                                ),
                                format!(
                                    "package = {} and module = {{}}",
                                    bytea_literal(p.as_slice())
                                ),
                                m
                            )
                        }
                    }
                };

                if let Some(sender) = sender {
                    filter!(
                        query,
                        format!("sender = {}", bytea_literal(sender.as_slice()))
                    )
                } else {
                    query
                }
            }
            (_, Some(_), Some(_)) => {
                return Err(Error::Internal(
                    "Filtering by both emitting module and event type is not supported".to_string(),
                ))
            }
        };

        query = add_ctes(query, &filter, &page);

        query = query
            .filter("(SELECT lo FROM tx_lo) <= tx_sequence_number")
            .filter("tx_sequence_number <= (SELECT hi FROM tx_hi)")
            .filter(
                "(ROW(tx_sequence_number, event_sequence_number) >= \
             ((SELECT lo FROM tx_lo), (SELECT lo FROM ev_lo)))",
            )
            .filter(
                "(ROW(tx_sequence_number, event_sequence_number) <= \
             ((SELECT hi FROM tx_hi), (SELECT hi FROM ev_hi)))",
            );

        // This is needed to make sure that if a transaction digest is specified, the corresponding
        // `tx_eq` must yield a non-empty result. Otherwise, the CTE setup will fallback to defaults
        // and we will return an unexpected response.
        if filter.transaction_digest.is_some() {
            query = filter!(query, "EXISTS (SELECT 1 FROM tx_eq)");
        }

        // hmmm... depending on how we need to handle checkpoint_viewed_at, we may need to make a fetch for its corresponding tx
        // in which case we might as well tack on some other things to fetch
        let (prev, next, results) = db
            .execute(move |conn| {
                // todo handle `checkpoint_viewed_at`

                let (prev, next, results) =
                    page.paginate_raw_query::<EvLookup>(conn, checkpoint_viewed_at, query)?;

                let ev_lookups = results
                    .into_iter()
                    .map(|x| (x.tx, x.ev))
                    .collect::<Vec<(i64, i64)>>();

                if ev_lookups.is_empty() {
                    return Ok::<_, diesel::result::Error>((prev, next, vec![]));
                }

                // Unlike a multi-get on a single column which can be serviced by a query `IN
                // (...)`, because events have a composite primary key, the query planner tends
                // to perform a sequential scan when given a list of tuples to lookup. A query
                // using `UNION ALL` allows us to leverage the index on the composite key.
                let mut events: Vec<StoredEvent> = conn.results(move || {
                    // Diesel's DSL does not current support chained `UNION ALL`, so we have to turn
                    // to `RawQuery` here.
                    let query_string = ev_lookups.iter()
                    .map(|&(tx, ev)| {
                        format!("SELECT * FROM events WHERE tx_sequence_number = {} AND event_sequence_number = {}", tx, ev)
                    })
                    .collect::<Vec<String>>()
                    .join(" UNION ALL ");

                    query!(query_string).into_boxed()
                })?;

                // UNION ALL does not guarantee order, so we need to sort the results. For now, we
                // always sort in ascending order.
                events.sort_by(|a, b| {
                        a.tx_sequence_number.cmp(&b.tx_sequence_number)
                            .then_with(|| a.event_sequence_number.cmp(&b.event_sequence_number))
                });


                Ok::<_, diesel::result::Error>((prev, next, events))
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

pub(crate) fn add_ctes(mut query: RawQuery, filter: &EventFilter, page: &Page<Cursor>) -> RawQuery {
    let mut ctes = vec![];

    if let Some(digest) = filter.transaction_digest {
        ctes.push(format!(
            r#"
            tx_eq AS (
                SELECT tx_sequence_number AS eq
                FROM tx_digests
                WHERE tx_digest = {}
            )
        "#,
            bytea_literal(digest.as_slice())
        ));
    }

    let (after_tx, after_ev) = page.after().map(|x| (x.tx, x.e)).unwrap_or((0, 0));

    let tx_lo = if !ctes.is_empty() {
        format!("SELECT GREATEST({}, (SELECT eq FROM tx_eq))", after_tx)
    } else {
        format!("SELECT {}", after_tx)
    };

    ctes.push(format!(
        r#"
        tx_lo AS ({} AS lo),
        ev_lo AS (
            SELECT CASE
                WHEN (SELECT lo FROM tx_lo) > {} THEN {} ELSE {}
            END AS lo
        )
    "#,
        tx_lo, after_tx, 0, after_ev
    ));

    let (before_tx, before_ev) = page
        .before()
        .map(|x| (x.tx, x.e))
        .unwrap_or((u64::MAX, u64::MAX));

    let tx_hi = if ctes.len() == 2 {
        format!("SELECT LEAST({}, (SELECT eq FROM tx_eq))", before_tx)
    } else {
        format!("SELECT {}", before_tx)
    };

    ctes.push(format!(
        r#"
        tx_hi AS ({} AS hi),
        ev_hi AS (
            SELECT CASE
                WHEN (SELECT hi FROM tx_hi) < {} THEN {} ELSE {}
            END AS hi
        )
    "#,
        tx_hi,
        before_tx,
        u64::MAX,
        before_ev
    ));

    for cte in ctes {
        query = query.with(cte);
    }

    query
}
