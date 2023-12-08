// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::dag_state::DagState;
use crate::metrics::PrimaryMetrics;
use crate::verifier::Verifier;
use anemo::Request;
use config::{AuthorityIdentifier, Committee};
use crypto::NetworkPublicKey;
use futures::{stream::FuturesUnordered, StreamExt};
use mysten_metrics::metered_channel::Sender;
use mysten_metrics::{monitored_future, monitored_scope};
use network::anemo_ext::NetworkExt as _;
use rand::{rngs::ThreadRng, seq::SliceRandom};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use sui_protocol_config::ProtocolConfig;
use tokio::sync::watch;
use tokio::{
    task::JoinSet,
    time::{sleep, timeout, Instant},
};
use tracing::{debug, info, instrument, trace, warn};
use types::{
    error::{DagError, DagResult},
    FetchHeadersRequest, FetchHeadersResponse, Round,
};
use types::{PrimaryToPrimaryClient, SignedHeader};

// Maximum number of headers to fetch with one request.
const MAX_HEADERS_TO_FETCH: usize = 2_000;
// Seconds to wait for a response before issuing another parallel fetch request.
const PARALLEL_FETCH_REQUEST_INTERVAL_SECS: Duration = Duration::from_secs(5);
// The timeout for an iteration of parallel fetch requests over all peers would be
// num peers * PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
const PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT: Duration = Duration::from_secs(15);

/// The HeaderGetter is responsible for fetching headers that this primary is missing
/// from peers. It is called to get specific headers.
pub(crate) struct HeaderGetter {
    state: Arc<HeaderFetcherState>,
}

/// Thread-safe internal state of HeaderFetcher shared with its fetch task.
struct HeaderFetcherState {
    /// Identity of the current authority.
    authority_id: AuthorityIdentifier,
    /// Committee of the current epoch.
    committee: Committee,
    /// Network client to fetch headers from other primaries.
    network: anemo::Network,
    /// Verifies then sends headers to Core for processing.
    verifier: Arc<Verifier>,
    /// Sends verified headers to Core for processing.
    tx_verified_headers: Sender<SignedHeader>,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
}

impl HeaderFetcher {
    pub fn new(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        network: anemo::Network,
        verifier: Arc<Verifier>,
        tx_verified_headers: Sender<SignedHeader>,
        metrics: Arc<PrimaryMetrics>,
    ) -> Self {
        let state = HeaderFetcherState {
            authority_id,
            committee,
            network,
            verifier,
            tx_verified_headers,
            metrics,
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub(crate) fn get_missing(&self, missing: Vec<HeaderKey>) -> JoinHandle<()> {
        spawn_monitored_task!(async move {
            let _scope = monitored_scope("Getter::task");
            let now = Instant::now();
            match run_get_task(
                state.clone(),
                missing,
            )
            .await
            {
                Ok(_) => {
                    debug!(
                        "Successfully finished task to fetch headers, elapsed = {}s",
                        now.elapsed().as_secs_f64()
                    );
                }
                Err(e) => {
                    warn!("Error from fetch headers task: {e}");
                }
            };
        })
    }
}

#[allow(clippy::mutable_key_type)]
#[instrument(level = "debug", skip_all)]
async fn run_get_task(
    state: Arc<HeaderGetterState>,
    missing: Vec<HeaderKey>,
) -> DagResult<()> {
    // Send request to fetch headers.
    let request = FetchHeadersRequest {
        exclusive_lower_bounds: last_header_rounds.into_iter().collect(),
        max_items: MAX_HEADERS_TO_FETCH,
    };

    let Some(response) =
        fetch_headers_helper(state.authority_id, &state.network, &committee, request).await
    else {
        return Err(DagError::NoHeaderFetched);
    };

    let num_fetched = response.headers.len() as u64;
    // Process headers.
    process_headers_helper(protocol_config, response, &state).await?;
    state
        .metrics
        .certificate_fetcher_num_certificates_processed
        .inc_by(num_fetched);

    debug!("Successfully fetched and processed {num_fetched} headers");

    return Ok(());
}

/// Fetches headers from other primaries concurrently, with ~5 sec interval between each request.
/// Terminates after the 1st successful response is received.
#[instrument(level = "debug", skip_all)]
async fn fetch_headers_helper(
    name: AuthorityIdentifier,
    network: &anemo::Network,
    committee: &Committee,
    request: FetchHeadersRequest,
) -> Option<FetchHeadersResponse> {
    let _scope = monitored_scope("Fetcher::request");
    trace!("Start sending fetch headers requests");
    // TODO: make this a config parameter.
    let request_interval = PARALLEL_FETCH_REQUEST_INTERVAL_SECS;
    let mut peers: Vec<NetworkPublicKey> = committee
        .others_primaries_by_id(name)
        .into_iter()
        .map(|(_, _, network_key)| network_key)
        .collect();
    peers.shuffle(&mut ThreadRng::default());
    let fetch_timeout = PARALLEL_FETCH_REQUEST_INTERVAL_SECS * peers.len().try_into().unwrap()
        + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT;
    let fetch_callback = async move {
        debug!("Starting to fetch headers");
        let mut fut = FuturesUnordered::new();
        // Loop until one peer returns with headers, or no peer does.
        loop {
            if let Some(peer) = peers.pop() {
                let request = Request::new(request.clone())
                    .with_timeout(PARALLEL_FETCH_REQUEST_INTERVAL_SECS * 2);
                let wait_peer = network.waiting_peer(anemo::PeerId(peer.0.to_bytes()));
                let mut client = PrimaryToPrimaryClient::new(wait_peer);
                fut.push(monitored_future!(async move {
                    debug!("Sending out fetch request in parallel to {peer}");
                    let result = client.fetch_headers(request).await;
                    if let Ok(resp) = &result {
                        debug!(
                            "Fetched {} headers from peer {peer}",
                            resp.body().headers.len()
                        );
                    }
                    result
                }));
            }
            let mut interval = Box::pin(sleep(request_interval));
            tokio::select! {
                res = fut.next() => match res {
                    Some(Ok(resp)) => {
                        if resp.body().headers.is_empty() {
                            // Issue request to another primary immediately.
                            continue;
                        }
                        return Some(resp.into_body());
                    }
                    Some(Err(e)) => {
                        debug!("Failed to fetch headers: {e:?}");
                        // Issue request to another primary immediately.
                        continue;
                    }
                    None => {
                        debug!("No peer can be reached for fetching headers!");
                        // Last or all requests to peers may have failed immediately, so wait
                        // before returning to avoid retrying fetching immediately.
                        sleep(request_interval).await;
                        return None;
                    }
                },
                _ = &mut interval => {
                    // Not response received in the last interval. Send out another fetch request
                    // in parallel, if there is a peer that has not been sent to.
                }
            };
        }
    };
    match timeout(fetch_timeout, fetch_callback).await {
        Ok(result) => result,
        Err(e) => {
            debug!("Timed out fetching headers: {e}");
            None
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn process_headers_helper(
    _protocol_config: &ProtocolConfig,
    response: FetchHeadersResponse,
    state: &HeaderFetcherState,
) -> DagResult<()> {
    trace!("Start sending fetched headers to processing");

    let _scope = monitored_scope("Fetcher::verify");
    for header in response.headers {
        state.verifier.verify(header).await?;
    }

    Ok(())
}
