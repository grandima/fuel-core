#![allow(non_snake_case)]
#![allow(clippy::cast_possible_truncation)]

use super::{
    FuelP2PService,
    PublishError,
};
use crate::{
    self as fuel_core_p2p,
    ports::P2PPreConfirmationMessage,
};
use fuel_core_p2p::{
    config::Config,
    gossipsub::{
        messages::{
            GossipTopicTag,
            GossipsubBroadcastRequest,
            GossipsubMessage,
        },
        topics::{
            NEW_TX_GOSSIP_TOPIC,
            TX_PRECONFIRMATIONS_GOSSIP_TOPIC,
        },
    },
    p2p_service::{
        FuelP2PEvent,
        GossipsubMessageHandler,
        RequestResponseMessageHandler,
    },
    peer_manager::PeerInfo,
    request_response::messages::{
        RequestMessage,
        ResponseError,
        ResponseSender,
        V2ResponseMessage,
    },
    service::to_message_acceptance,
};
use fuel_core_types::{
    blockchain::{
        SealedBlockHeader,
        consensus::{
            Consensus,
            poa::PoAConsensus,
        },
        header::BlockHeader,
    },
    fuel_tx::{
        Transaction,
        TransactionBuilder,
        TxId,
        UniqueIdentifier,
    },
    fuel_types::ChainId,
    services::p2p::{
        GossipsubMessageAcceptance,
        NetworkableTransactionPool,
        Transactions,
    },
};
use futures::{
    StreamExt,
    future::join_all,
};
use libp2p::{
    Multiaddr,
    PeerId,
    gossipsub::{
        Sha256Topic,
        Topic,
    },
    identity::Keypair,
    swarm::{
        ListenError,
        SwarmEvent,
    },
};
use rand::Rng;
use std::{
    collections::HashSet,
    ops::{
        Deref,
        Range,
    },
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    broadcast,
    mpsc,
    oneshot,
    watch,
};
use tracing_attributes::instrument;
type P2PService = FuelP2PService;

/// helper function for building FuelP2PService
async fn build_service_from_config(mut p2p_config: Config) -> P2PService {
    p2p_config.keypair = Keypair::generate_secp256k1(); // change keypair for each Node
    let max_block_size = p2p_config.max_block_size;
    let (sender, _) =
        broadcast::channel(p2p_config.reserved_nodes.len().saturating_add(1));

    let mut service = FuelP2PService::new(
        sender,
        p2p_config,
        GossipsubMessageHandler::new(),
        RequestResponseMessageHandler::new(max_block_size),
    )
    .await
    .unwrap();
    service.start().await.unwrap();
    service
}

async fn setup_bootstrap_nodes(
    p2p_config: &Config,
    bootstrap_nodes_count: usize,
) -> (Vec<P2PService>, Vec<Multiaddr>) {
    let nodes = join_all(
        (0..bootstrap_nodes_count).map(|_| build_service_from_config(p2p_config.clone())),
    )
    .await;
    let bootstrap_multiaddrs = nodes
        .iter()
        .flat_map(|b| b.multiaddrs())
        .collect::<Vec<_>>();
    (nodes, bootstrap_multiaddrs)
}

fn spawn(stop: &watch::Sender<()>, mut node: P2PService) {
    let mut stop = stop.subscribe();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = node.next_event() => {}
                _ = stop.changed() => {
                    break;
                }
            }
        }
    });
}

#[tokio::test]
#[instrument]
async fn p2p_service_works() {
    build_service_from_config(Config::default_initialized("p2p_service_works")).await;
}

// Single sentry node connects to multiple reserved nodes and `max_peers_allowed` amount of non-reserved nodes.
// It also tries to dial extra non-reserved nodes to establish the connection.
// A single reserved node is not started immediately with the rest of the nodes.
// Once sentry node establishes the connection with the allowed number of nodes
// we start the reserved node, and await for it to establish the connection.
// This test proves that there is always an available slot for the reserved node to connect to.
#[tokio::test(flavor = "multi_thread")]
#[instrument]
async fn reserved_nodes_reconnect_works() {
    let p2p_config = Config::default_initialized("reserved_nodes_reconnect_works");

    // total amount will be `max_peers_allowed` + `reserved_nodes.len()`
    let max_peers_allowed: usize = 3;

    let (bootstrap_nodes, bootstrap_multiaddrs) =
        setup_bootstrap_nodes(&p2p_config, max_peers_allowed.saturating_mul(5)).await;
    let (mut reserved_nodes, reserved_multiaddrs) =
        setup_bootstrap_nodes(&p2p_config, max_peers_allowed).await;

    let mut sentry_node = {
        let mut p2p_config = p2p_config.clone();
        p2p_config.max_functional_peers_connected = max_peers_allowed as u32;

        p2p_config.bootstrap_nodes = bootstrap_multiaddrs;

        p2p_config.reserved_nodes = reserved_multiaddrs;

        build_service_from_config(p2p_config).await
    };

    // pop() a single reserved node, so it's not run with the rest of the nodes
    let mut reserved_node = reserved_nodes.pop();
    let reserved_node_peer_id = reserved_node.as_ref().unwrap().local_peer_id;

    let all_node_services: Vec<_> = bootstrap_nodes
        .into_iter()
        .chain(reserved_nodes.into_iter())
        .collect();

    let mut all_nodes_ids: Vec<PeerId> = all_node_services
        .iter()
        .map(|service| service.local_peer_id)
        .collect();

    let (stop_sender, _) = watch::channel(());
    all_node_services.into_iter().for_each(|node| {
        spawn(&stop_sender, node);
    });

    loop {
        tokio::select! {
            sentry_node_event = sentry_node.next_event() => {
                // we've connected to all other peers
                if sentry_node.peer_manager.total_peers_connected() > max_peers_allowed {
                    // if the `reserved_node` is not included,
                    // create and insert it, to be polled with rest of the nodes
                    if !all_nodes_ids
                    .iter()
                    .any(|local_peer_id| local_peer_id == &reserved_node_peer_id) {
                        if let Some(node) = reserved_node {
                            all_nodes_ids.push(node.local_peer_id);
                            spawn(&stop_sender, node);
                            reserved_node = None;
                        }
                    }
                }
                if let Some(FuelP2PEvent::PeerConnected(peer_id)) = sentry_node_event {
                    // we connected to the desired reserved node
                    if peer_id == reserved_node_peer_id {
                        break
                    }
                }
            },
        }
    }
    stop_sender.send(()).unwrap();
}

#[tokio::test]
#[instrument]
async fn dont_connect_to_node_with_same_peer_id() {
    let mut p2p_config =
        Config::default_initialized("dont_connect_to_node_with_same_peer_id");
    let mut node_a = build_service_from_config(p2p_config.clone()).await;
    // We don't use build_service_from_config here, because we want to use the same keypair
    // to have the same PeerId
    let node_b = {
        // Given
        p2p_config.reserved_nodes = node_a.multiaddrs();
        let max_block_size = p2p_config.max_block_size;
        let (sender, _) =
            broadcast::channel(p2p_config.reserved_nodes.len().saturating_add(1));

        let mut service = FuelP2PService::new(
            sender,
            p2p_config,
            GossipsubMessageHandler::new(),
            RequestResponseMessageHandler::new(max_block_size),
        )
        .await
        .unwrap();
        service.start().await.unwrap();
        service
    };
    // When
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let event = node_a.next_event().await;
            if let Some(FuelP2PEvent::PeerConnected(_)) = event {
                panic!("Node B should not connect to Node A because they have the same PeerId");
            }
            assert_eq!(node_a.peer_manager().total_peers_connected(), 0);
        }
    })
        .await
        // Then
        .expect_err("The node should not connect to itself");
    assert_eq!(node_b.peer_manager().total_peers_connected(), 0);
}

// We start with two nodes, node_a and node_b, bootstrapped with `bootstrap_nodes_count` other nodes.
// Yet node_a and node_b are only allowed to connect to specified amount of nodes.
#[tokio::test]
#[instrument]
async fn max_peers_connected_works() {
    let p2p_config = Config::default_initialized("max_peers_connected_works");

    let bootstrap_nodes_count = 20;
    let node_a_max_peers_allowed: usize = 3;
    let node_b_max_peers_allowed: usize = 5;

    let (mut nodes, nodes_multiaddrs) =
        setup_bootstrap_nodes(&p2p_config, bootstrap_nodes_count).await;

    // this node is allowed to only connect to `node_a_max_peers_allowed` other nodes
    let mut node_a = {
        let mut p2p_config = p2p_config.clone();
        p2p_config.max_discovery_peers_connected = node_a_max_peers_allowed as u32;
        // it still tries to dial all nodes!
        p2p_config.bootstrap_nodes.clone_from(&nodes_multiaddrs);

        build_service_from_config(p2p_config).await
    };

    // this node is allowed to only connect to `node_b_max_peers_allowed` other nodes
    let mut node_b = {
        let mut p2p_config = p2p_config.clone();
        p2p_config.max_discovery_peers_connected = node_b_max_peers_allowed as u32;
        // it still tries to dial all nodes!
        p2p_config.bootstrap_nodes.clone_from(&nodes_multiaddrs);

        build_service_from_config(p2p_config).await
    };

    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    let jh = tokio::spawn(async move {
        while rx.try_recv().is_err() {
            futures::stream::iter(nodes.iter_mut())
                .for_each_concurrent(4, |node| async move {
                    node.next_event().await;
                })
                .await;
        }
    });

    let mut node_a_hit_limit = false;
    let mut node_b_hit_limit = false;
    let mut instance = tokio::time::Instant::now();

    // After we hit limit for node_a and node_b start timer.
    // If we don't exceed the limit during 5 seconds, finish the test successfully.
    while instance.elapsed().as_secs() < 5 {
        tokio::select! {
            event_from_node_a = node_a.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(_)) = event_from_node_a {
                    if node_a.peer_manager().total_peers_connected() > node_a_max_peers_allowed {
                        panic!("The node should only connect to max {node_a_max_peers_allowed} peers");
                    }
                    node_a_hit_limit |= node_a.peer_manager().total_peers_connected() == node_a_max_peers_allowed;
                }
                tracing::info!("Event from the node_a: {:?}", event_from_node_a);
            },
            event_from_node_b = node_b.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(_)) = event_from_node_b {
                    if node_b.peer_manager().total_peers_connected() > node_b_max_peers_allowed {
                        panic!("The node should only connect to max {node_b_max_peers_allowed} peers");
                    }
                    node_b_hit_limit |= node_b.peer_manager().total_peers_connected() == node_b_max_peers_allowed;
                }
                tracing::info!("Event from the node_b: {:?}", event_from_node_b);
            },
        }

        if !(node_a_hit_limit && node_b_hit_limit) {
            instance = tokio::time::Instant::now();
        }
    }

    tx.send(()).unwrap();
    jh.await.unwrap()
}

// Simulate 2 Sets of Sentry nodes.
// In both Sets, a single Guarded Node should only be connected to their sentry nodes.
// While other nodes can and should connect to nodes outside of the Sentry Set.
#[tokio::test(flavor = "multi_thread")]
#[instrument]
async fn sentry_nodes_working() {
    const RESERVED_NODE_SIZE: usize = 4;

    let mut p2p_config = Config::default_initialized("sentry_nodes_working");

    async fn build_sentry_nodes(p2p_config: Config) -> (P2PService, Vec<P2PService>) {
        let (reserved_nodes, reserved_multiaddrs) =
            setup_bootstrap_nodes(&p2p_config, RESERVED_NODE_SIZE).await;

        // set up the guraded node service with `reserved_nodes_only_mode`
        let guarded_node_service = {
            let mut p2p_config = p2p_config.clone();
            p2p_config.reserved_nodes = reserved_multiaddrs;
            p2p_config.reserved_nodes_only_mode = true;
            build_service_from_config(p2p_config).await
        };

        let sentry_nodes = reserved_nodes;

        (guarded_node_service, sentry_nodes)
    }

    let (mut first_guarded_node, mut first_sentry_nodes) =
        build_sentry_nodes(p2p_config.clone()).await;
    p2p_config.bootstrap_nodes = first_sentry_nodes
        .iter()
        .flat_map(|n| n.multiaddrs())
        .collect();

    let (mut second_guarded_node, second_sentry_nodes) =
        build_sentry_nodes(p2p_config).await;

    let first_sentry_set: HashSet<_> = first_sentry_nodes
        .iter()
        .map(|node| node.local_peer_id)
        .collect();

    let second_sentry_set: HashSet<_> = second_sentry_nodes
        .iter()
        .map(|node| node.local_peer_id)
        .collect();

    let mut single_sentry_node = first_sentry_nodes.pop().unwrap();
    let mut sentry_node_connections = HashSet::new();
    let (stop_sender, _) = watch::channel(());
    first_sentry_nodes
        .into_iter()
        .chain(second_sentry_nodes.into_iter())
        .for_each(|node| {
            spawn(&stop_sender, node);
        });

    let mut instance = tokio::time::Instant::now();
    // After guards are connected to all sentries and at least one sentry has
    // more connections than sentries in the group, start the timer..
    // If guards don't connected to new nodes during 5 seconds, finish the test successfully.
    while instance.elapsed().as_secs() < 5 {
        tokio::select! {
            event_from_first_guarded = first_guarded_node.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(peer_id)) = event_from_first_guarded {
                    if !first_sentry_set.contains(&peer_id) {
                        panic!("The node should only connect to the specified reserved nodes!");
                    }
                }
                tracing::info!("Event from the first guarded node: {:?}", event_from_first_guarded);
            },
            event_from_second_guarded = second_guarded_node.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(peer_id)) = event_from_second_guarded {
                    if !second_sentry_set.contains(&peer_id) {
                        panic!("The node should only connect to the specified reserved nodes!");
                    }
                }
                tracing::info!("Event from the second guarded node: {:?}", event_from_second_guarded);
            },
            // Poll one of the reserved, sentry nodes
            sentry_node_event = single_sentry_node.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(peer_id)) = sentry_node_event {
                    sentry_node_connections.insert(peer_id);
                }
            }
        };

        // This reserved node has connected to more than the number of reserved nodes it is part of.
        // It means it has discovered other nodes in the network.
        if sentry_node_connections.len() < 2 * RESERVED_NODE_SIZE {
            instance = tokio::time::Instant::now();
        }
    }
    stop_sender.send(()).unwrap();
}

// Simulates 2 p2p nodes that are on the same network and should connect via mDNS
// without any additional bootstrapping
#[tokio::test]
#[instrument]
async fn nodes_connected_via_mdns() {
    // Node A
    let mut p2p_config = Config::default_initialized("nodes_connected_via_mdns");
    p2p_config.enable_mdns = true;
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    let mut node_b = build_service_from_config(p2p_config).await;

    loop {
        tokio::select! {
            node_b_event = node_b.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(_)) = node_b_event {
                    // successfully connected to Node A
                    break
                }
                tracing::info!("Node B Event: {:?}", node_b_event);
            },
            _ = node_a.swarm.select_next_some() => {},
        };
    }
}

// Simulates 2 p2p nodes that are on the same network but their Fuel Upgrade checksum is different
// (different chain id or chain config)
// So they are not able to connect
#[tokio::test]
#[instrument]
async fn nodes_cannot_connect_due_to_different_checksum() {
    use libp2p::TransportError;
    // Node A
    let mut p2p_config =
        Config::default_initialized("nodes_cannot_connect_due_to_different_checksum");
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // different checksum
    p2p_config.checksum = [1u8; 32].into();
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    // Node B
    let mut node_b = build_service_from_config(p2p_config).await;

    loop {
        tokio::select! {
            node_a_event = node_a.swarm.select_next_some() => {
                tracing::info!("Node A Event: {:?}", node_a_event);
                if let SwarmEvent::IncomingConnectionError { error: ListenError::Transport(TransportError::Other(_)), .. } = node_a_event {
                    break
                }
            },
            node_b_event = node_b.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(_)) = node_b_event {
                    panic!("Node B should not connect to Node A!")
                }
                tracing::info!("Node B Event: {:?}", node_b_event);
            },

        };
    }
}

// Simulates 3 p2p nodes, Node B & Node C are bootstrapped with Node A
// Using Identify Protocol Node C should be able to identify and connect to Node B
#[tokio::test]
#[instrument]
async fn nodes_connected_via_identify() {
    // Node A
    let mut p2p_config = Config::default_initialized("nodes_connected_via_identify");

    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    // Node C
    let mut node_c = build_service_from_config(p2p_config).await;

    loop {
        tokio::select! {
            node_a_event = node_a.next_event() => {
                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            node_b_event = node_b.next_event() => {
                tracing::info!("Node B Event: {:?}", node_b_event);
            },

            node_c_event = node_c.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(peer_id)) = node_c_event {
                    // we have connected to Node B!
                    if peer_id == node_b.local_peer_id {
                        break
                    }
                }

                tracing::info!("Node C Event: {:?}", node_c_event);
            }
        };
    }
}

// Simulates 2 p2p nodes that connect to each other and consequently exchange Peer Info
// On successful connection, node B updates its latest BlockHeight
// and shares it with Peer A via Heartbeat protocol
#[tokio::test]
#[instrument]
async fn peer_info_updates_work() {
    let mut p2p_config = Config::default_initialized("peer_info_updates_work");

    // Node A
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config).await;

    let latest_block_height = 40_u32.into();

    loop {
        tokio::select! {
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::PeerInfoUpdated { peer_id, block_height: _ }) = node_a_event {
                    if let Some(PeerInfo {  heartbeat_data, client_version, .. }) = node_a.peer_manager.get_peer_info(&peer_id) {
                        // Exits after it verifies that:
                        // 1. Peer Addresses are known
                        // 2. Client Version is known
                        // 3. Node has responded with their latest BlockHeight
                        if client_version.is_some() && heartbeat_data.block_height == Some(latest_block_height) {
                            break;
                        }
                    }
                }

                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            node_b_event = node_b.next_event() => {
                if let Some(FuelP2PEvent::PeerConnected(_)) = node_b_event {
                    // we've connected to Peer A
                    // let's update our BlockHeight
                    node_b.update_block_height(latest_block_height);
                }

                tracing::info!("Node B Event: {:?}", node_b_event);
            }
        }
    }
}

#[tokio::test]
#[instrument]
async fn gossipsub_broadcast_tx_with_accept__new_tx() {
    for _ in 0..100 {
        tokio::time::timeout(
            Duration::from_secs(5),
            gossipsub_broadcast(
                GossipsubBroadcastRequest::NewTx(
                    Arc::new(Transaction::default_test_tx()),
                ),
                GossipsubMessageAcceptance::Accept,
                None,
            ),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[instrument]
async fn gossipsub_broadcast_tx_with_accept__tx_preconfirmations() {
    for _ in 0..100 {
        tokio::time::timeout(
            Duration::from_secs(20),
            gossipsub_broadcast(
                GossipsubBroadcastRequest::TxPreConfirmations(Arc::new(
                    P2PPreConfirmationMessage::default_test_confirmation(),
                )),
                GossipsubMessageAcceptance::Accept,
                None,
            ),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[instrument]
async fn gossipsub_broadcast_tx_with_reject__new_tx() {
    for _ in 0..100 {
        tokio::time::timeout(
            Duration::from_secs(5),
            gossipsub_broadcast(
                GossipsubBroadcastRequest::NewTx(
                    Arc::new(Transaction::default_test_tx()),
                ),
                GossipsubMessageAcceptance::Reject,
                None,
            ),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[instrument]
async fn gossipsub_broadcast_tx_with_reject__tx_preconfirmations() {
    for _ in 0..100 {
        tokio::time::timeout(
            Duration::from_secs(5),
            gossipsub_broadcast(
                GossipsubBroadcastRequest::TxPreConfirmations(Arc::new(
                    P2PPreConfirmationMessage::default_test_confirmation(),
                )),
                GossipsubMessageAcceptance::Reject,
                None,
            ),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
#[instrument]
#[ignore]
async fn gossipsub_scoring_with_accepted_messages() {
    gossipsub_scoring_tester(
        "gossipsub_scoring_with_accepted_messages",
        100,
        GossipsubMessageAcceptance::Accept,
    )
    .await;
}

/// At `GRAYLIST_THRESHOLD` the node will ignore all messages from the peer
/// And our PeerManager will ban the peer at that point - leading to disconnect
#[tokio::test]
#[instrument]
#[ignore]
async fn gossipsub_scoring_with_rejected_messages() {
    gossipsub_scoring_tester(
        "gossipsub_scoring_with_rejected_messages",
        100,
        GossipsubMessageAcceptance::Reject,
    )
    .await;
}

// TODO: Move me before tests that use this function
/// Helper function for testing gossipsub scoring
/// ! Dev Note: this function runs forever, its purpose is to show the scoring in action with passage of time
async fn gossipsub_scoring_tester(
    test_name: &str,
    amount_of_msgs_per_second: usize,
    acceptance: GossipsubMessageAcceptance,
) {
    let mut p2p_config = Config::default_initialized(test_name);

    // Node A
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    // Node C
    p2p_config.bootstrap_nodes = node_b.multiaddrs();
    let mut node_c = build_service_from_config(p2p_config.clone()).await;

    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::GossipsubMessage { message_id, peer_id, .. }) = node_a_event {
                    let msg_acceptance = to_message_acceptance(&acceptance);
                    node_a.report_message_validation_result(&message_id, peer_id, msg_acceptance);
                }
            }
            node_b_event = node_b.next_event() => {
                if let Some(FuelP2PEvent::GossipsubMessage { message_id, peer_id, .. }) = node_b_event {
                    let msg_acceptance = to_message_acceptance(&acceptance);
                    node_b.report_message_validation_result(&message_id, peer_id, msg_acceptance);
                }
            },
            node_c_event = node_c.next_event() => {
                if let Some(FuelP2PEvent::GossipsubMessage { message_id, peer_id, .. }) = node_c_event {
                    let msg_acceptance = to_message_acceptance(&acceptance);
                    node_c.report_message_validation_result(&message_id, peer_id, msg_acceptance);
                }
            },
            _ = interval.tick() => {
                let mut transactions = vec![];
                for _ in 0..amount_of_msgs_per_second {
                    let random_tx =
                        TransactionBuilder::script(rand::thread_rng().r#gen::<[u8; 32]>().to_vec(), rand::thread_rng().r#gen::<[u8; 32]>().to_vec()).finalize_as_transaction();

                    transactions.push(random_tx.clone());
                    let random_tx = GossipsubBroadcastRequest::NewTx(Arc::new(random_tx));

                    match rand::thread_rng().gen_range(1..=3) {
                        1 => {
                            // Node A sends a Transaction
                            let _ = node_a.publish_message(random_tx);

                        },
                        2 => {
                            // Node B sends a Transaction
                            let _ = node_b.publish_message(random_tx);

                        },
                        3 => {
                            // Node C sends a Transaction
                            let _ = node_c.publish_message(random_tx);
                        },
                        _ => unreachable!("Random number generator is broken")
                    }
                }

                eprintln!("Node A WORLD VIEW");
                eprintln!("B score: {:?}", node_a.get_peer_score(&node_b.local_peer_id).unwrap());
                eprintln!("C score: {:?}", node_a.get_peer_score(&node_c.local_peer_id).unwrap());
                eprintln!();

                eprintln!("Node B WORLD VIEW");
                eprintln!("A score: {:?}", node_b.get_peer_score(&node_a.local_peer_id).unwrap());
                eprintln!("C score: {:?}", node_b.get_peer_score(&node_c.local_peer_id).unwrap());
                eprintln!();

                eprintln!("Node C WORLD VIEW");
                eprintln!("A score: {:?}", node_c.get_peer_score(&node_a.local_peer_id).unwrap());
                eprintln!("B score: {:?}", node_c.get_peer_score(&node_b.local_peer_id).unwrap());
                eprintln!();

                // never ending loop
                // break;
            }
        }
    }
}

// TODO: Move me before tests that use this function
/// Reusable helper function for Broadcasting Gossipsub requests
async fn gossipsub_broadcast(
    broadcast_request: GossipsubBroadcastRequest,
    acceptance: GossipsubMessageAcceptance,
    connection_limit: Option<u32>,
) {
    let mut p2p_config = Config::default_initialized("gossipsub_exchanges_messages");

    if let Some(connection_limit) = connection_limit {
        p2p_config.max_functional_peers_connected = connection_limit;
    }

    p2p_config.subscribe_to_new_tx = true;
    
    p2p_config.subscribe_to_pre_confirmations = true;

    let (selected_topic, selected_tag): (Sha256Topic, GossipTopicTag) = {
        let (topic, tag) = match broadcast_request {
            GossipsubBroadcastRequest::NewTx(_) => {
                (NEW_TX_GOSSIP_TOPIC, GossipTopicTag::NewTx)
            }
            GossipsubBroadcastRequest::TxPreConfirmations(_) => (
                TX_PRECONFIRMATIONS_GOSSIP_TOPIC,
                GossipTopicTag::TxPreconfirmations,
            ),
        };

        (
            Topic::new(format!("{}/{}", topic, p2p_config.network_name)),
            tag,
        )
    };
    tracing::info!("Selected Topic: {:?}", selected_topic);

    let mut message_sent = false;

    // Node A
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    // Node C
    p2p_config.bootstrap_nodes = node_b.multiaddrs();
    let mut node_c = build_service_from_config(p2p_config.clone()).await;

    // Node C does not connect to Node A
    // it should receive the propagated message from Node B if `GossipsubMessageAcceptance` is `Accept`
    node_c
        .swarm
        .behaviour_mut()
        .block_peer(node_a.local_peer_id);

    let mut a_connected_to_b = false;
    let mut b_connected_to_c = false;
    loop {
        // verifies that we've got at least a single peer address to send message to
        if a_connected_to_b && b_connected_to_c && !message_sent {
            message_sent = true;
            let broadcast_request = broadcast_request.clone();
            node_a.publish_message(broadcast_request).unwrap();
        }

        tokio::select! {
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::NewSubscription { peer_id, tag }) = &node_a_event {
                    if tag != &selected_tag {
                        tracing::info!("Wrong tag, expected: {:?}, actual: {:?}", selected_tag, tag);
                    } else if peer_id == &node_b.local_peer_id {
                        a_connected_to_b = true;
                    }
                }
                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            node_b_event = node_b.next_event() => {
                if let Some(FuelP2PEvent::NewSubscription { peer_id,tag,  }) = &node_b_event {
                    tracing::info!("New subscription for peer_id: {:?} with tag: {:?}", peer_id, tag);
                    if tag != &selected_tag {
                        tracing::info!("Wrong tag, expected: {:?}, actual: {:?}", selected_tag, tag);
                    } else if peer_id == &node_c.local_peer_id {
                        b_connected_to_c = true;
                    }
                }

                if let Some(FuelP2PEvent::GossipsubMessage { topic_hash, message, message_id, peer_id }) = node_b_event.clone() {
                    // Message Validation must be reported
                    // If it's `Accept`, Node B will propagate the message to Node C
                    // If it's `Ignore` or `Reject`, Node C should not receive anything
                    let msg_acceptance = to_message_acceptance(&acceptance);
                    node_b.report_message_validation_result(&message_id, peer_id, msg_acceptance);
                    if topic_hash != selected_topic.hash() {
                        tracing::error!("Wrong topic hash, expected: {} - actual: {}", selected_topic.hash(), topic_hash);
                        panic!("Wrong Topic");
                    }

                    check_message_matches_request(&message, &broadcast_request);

                    // Node B received the correct message
                    // If we try to publish it again we will get `PublishError::Duplicate`
                    // This asserts that our MessageId calculation is consistent irrespective of which Peer sends it
                    let broadcast_request = broadcast_request.clone();
                    matches!(node_b.publish_message(broadcast_request), Err(PublishError::Duplicate));

                    match acceptance {
                        GossipsubMessageAcceptance::Reject | GossipsubMessageAcceptance::Ignore => {
                            break
                        },
                        _ => {
                            // the `exit` should happen in Node C
                        }
                    }
                }

                tracing::info!("Node B Event: {:?}", node_b_event);
            }

            node_c_event = node_c.next_event() => {
                if let Some(FuelP2PEvent::GossipsubMessage { peer_id, .. }) = node_c_event.clone() {
                    // Node B should be the source propagator
                    assert!(peer_id == node_b.local_peer_id);
                    match acceptance {
                        GossipsubMessageAcceptance::Reject | GossipsubMessageAcceptance::Ignore => {
                            panic!("Node C should not receive Rejected or Ignored messages")
                        },
                        GossipsubMessageAcceptance::Accept => {
                            break
                        }
                    }
                }
            }
        };
    }
}

fn check_message_matches_request(
    message: &GossipsubMessage,
    expected: &GossipsubBroadcastRequest,
) {
    match (message, expected) {
        (
            GossipsubMessage::NewTx(received),
            GossipsubBroadcastRequest::NewTx(requested),
        ) => {
            assert_eq!(
                requested.deref(),
                received,
                "Both messages were `NewTx`s, but the received message did not match the requested message"
            );
        }
        (
            GossipsubMessage::TxPreConfirmations(received),
            GossipsubBroadcastRequest::TxPreConfirmations(requested),
        ) => assert_eq!(
            requested.deref(),
            received,
            "Both messages were `Preconfirmations`, but the received message did not match the requested message"
        ),
        _ => panic!(
            "Message does not match the expected request, expected: {:?}, actual: {:?}",
            expected, message
        ),
    }
}

fn arbitrary_headers_for_range(range: Range<u32>) -> Vec<SealedBlockHeader> {
    let mut blocks = Vec::new();
    for i in range {
        let mut header: BlockHeader = Default::default();
        header.set_block_height(i.into());

        let sealed_block = SealedBlockHeader {
            entity: header,
            consensus: Consensus::PoA(PoAConsensus::new(Default::default())),
        };
        blocks.push(sealed_block);
    }
    blocks
}

// Metadata gets skipped during serialization, so this is the fuzzy way to compare blocks
fn eq_except_metadata(a: &SealedBlockHeader, b: &SealedBlockHeader) -> bool {
    let app_eq = match (&a.entity, &b.entity) {
        (BlockHeader::V1(a), BlockHeader::V1(b)) => a.application() == b.application(),
        #[cfg(feature = "fault-proving")]
        (BlockHeader::V2(a), BlockHeader::V2(b)) => a.application() == b.application(),
        #[cfg_attr(not(feature = "fault-proving"), allow(unreachable_patterns))]
        _ => false,
    };
    app_eq && a.entity.consensus() == b.entity.consensus()
}

async fn request_response_works_with(
    request_msg: RequestMessage,
    connection_limit: Option<u32>,
) {
    let mut p2p_config = Config::default_initialized("request_response_works_with");

    if let Some(connection_limit) = connection_limit {
        p2p_config.max_functional_peers_connected = connection_limit;
    }

    // Node A
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    let (tx_test_end, mut rx_test_end) = mpsc::channel::<bool>(1);

    let mut request_sent = false;

    loop {
        tokio::select! {
            message_sent = rx_test_end.recv() => {
                // we received a signal to end the test
                assert!(message_sent.unwrap(), "Received incorrect or missing message");
                break;
            }
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::PeerInfoUpdated { peer_id, block_height: _ }) = node_a_event {
                    if node_a.peer_manager.get_peer_info(&peer_id).is_some() {
                        // 0. verifies that we've got at least a single peer address to request message from
                        if !request_sent {
                            request_sent = true;

                            match request_msg.clone() {
                                RequestMessage::SealedHeaders(range) => {
                                    let (tx_orchestrator, rx_orchestrator) = oneshot::channel();
                                    assert!(node_a.send_request_msg(None, request_msg.clone(), ResponseSender::SealedHeaders(tx_orchestrator)).is_ok());
                                    let tx_test_end = tx_test_end.clone();

                                    tokio::spawn(async move {
                                        let response_message = rx_orchestrator.await;

                                        let expected = arbitrary_headers_for_range(range.clone());

                                        if let Ok(response) = response_message {
                                            match response {
                                                Ok((_, Ok(Ok(sealed_headers)))) => {
                                                    let check = expected.iter().zip(sealed_headers.iter()).all(|(a, b)| eq_except_metadata(a, b));
                                                    let _ = tx_test_end.send(check).await;
                                                },
                                                Ok((_, Ok(Err(e)))) => {
                                                    tracing::error!("Node A did not return any headers: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                                Ok((_, Err(e))) => {
                                                    tracing::error!("Error in P2P communication: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                                Err(e) => {
                                                    tracing::error!("Error in P2P before sending message: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                            }
                                        } else {
                                            tracing::error!("Orchestrator failed to receive a message: {:?}", response_message);
                                            let _ = tx_test_end.send(false).await;
                                        }
                                    });
                                }
                                RequestMessage::Transactions(_range) => {
                                    let (tx_orchestrator, rx_orchestrator) = oneshot::channel();
                                    assert!(node_a.send_request_msg(None, request_msg.clone(), ResponseSender::Transactions(tx_orchestrator)).is_ok());
                                    let tx_test_end = tx_test_end.clone();

                                    tokio::spawn(async move {
                                        let response_message = rx_orchestrator.await;

                                        if let Ok(response) = response_message {
                                            match response {
                                                Ok((_, Ok(Ok(transactions)))) => {
                                                    let check = transactions.len() == 1 && transactions[0].0.len() == 5;
                                                    let _ = tx_test_end.send(check).await;
                                                },
                                                Ok((_, Ok(Err(e)))) => {
                                                    tracing::error!("Node A did not return any transactions: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                                Ok((_, Err(e))) => {
                                                    tracing::error!("Error in P2P communication: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                                Err(e) => {
                                                    tracing::error!("Error in P2P before sending message: {:?}", e);
                                                    let _ = tx_test_end.send(false).await;
                                                },
                                            }
                                        } else {
                                            tracing::error!("Orchestrator failed to receive a message: {:?}", response_message);
                                            let _ = tx_test_end.send(false).await;
                                        }
                                    });
                                }
                                RequestMessage::TxPoolAllTransactionsIds => {
                                    let (tx_orchestrator, rx_orchestrator) = oneshot::channel();
                                    assert!(node_a.send_request_msg(None, request_msg.clone(), ResponseSender::TxPoolAllTransactionsIds(tx_orchestrator)).is_ok());
                                    let tx_test_end = tx_test_end.clone();
                                    tokio::spawn(async move {
                                        let response_message = rx_orchestrator.await;

                                        if let Ok((_, Ok(Ok(transaction_ids)))) = response_message {
                                            let tx_ids: Vec<TxId> = (0..5).map(|_| Transaction::default_test_tx().id(&ChainId::new(1))).collect();
                                            let check = transaction_ids.len() == 5 && transaction_ids.iter().zip(tx_ids.iter()).all(|(a, b)| a == b);
                                            let _ = tx_test_end.send(check).await;
                                        } else {
                                            tracing::error!("Orchestrator failed to receive a message: {:?}", response_message);
                                            let _ = tx_test_end.send(false).await;
                                        }
                                    });
                                }
                                RequestMessage::TxPoolFullTransactions(tx_ids) => {
                                    let (tx_orchestrator, rx_orchestrator) = oneshot::channel();
                                    assert!(node_a.send_request_msg(None, request_msg.clone(), ResponseSender::TxPoolFullTransactions(tx_orchestrator)).is_ok());
                                    let tx_test_end = tx_test_end.clone();
                                    tokio::spawn(async move {
                                        let response_message = rx_orchestrator.await;

                                        if let Ok((_, Ok(Ok(transactions)))) = response_message {
                                            let txs: Vec<Option<NetworkableTransactionPool>> = tx_ids.iter().enumerate().map(|(i, _)| {
                                                if i == 0 {
                                                    None
                                                } else {
                                                    Some(NetworkableTransactionPool::Transaction(Transaction::default_test_tx()))
                                                }
                                            }).collect();
                                            let check = transactions.len() == tx_ids.len() && transactions.iter().zip(txs.iter()).all(|(a, b)| a == b);
                                            let _ = tx_test_end.send(check).await;
                                        } else {
                                            tracing::error!("Orchestrator failed to receive a message: {:?}", response_message);
                                            let _ = tx_test_end.send(false).await;
                                        }
                                    });
                                }
                            }
                        }
                    }
                }

                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            node_b_event = node_b.next_event() => {
                // 2. Node B receives the RequestMessage from Node A initiated by the NetworkOrchestrator
                if let Some(FuelP2PEvent::InboundRequestMessage{ request_id, request_message: received_request_message }) = &node_b_event {
                    match received_request_message {
                        RequestMessage::SealedHeaders(range) => {
                            let sealed_headers: Vec<_> = arbitrary_headers_for_range(range.clone());

                            let _ = node_b.send_response_msg(*request_id, V2ResponseMessage::SealedHeaders(Ok(sealed_headers)));
                        }
                        RequestMessage::Transactions(_) => {
                            let txs = (0..5).map(|_| Transaction::default_test_tx()).collect();
                            let transactions = vec![Transactions(txs)];
                            let _ = node_b.send_response_msg(*request_id, V2ResponseMessage::Transactions(Ok(transactions)));
                        }
                        RequestMessage::TxPoolAllTransactionsIds => {
                            let tx_ids = (0..5).map(|_| Transaction::default_test_tx().id(&ChainId::new(1))).collect();
                            let _ = node_b.send_response_msg(*request_id, V2ResponseMessage::TxPoolAllTransactionsIds(Ok(tx_ids)));
                        }
                        RequestMessage::TxPoolFullTransactions(tx_ids) => {
                            let txs = tx_ids.iter().enumerate().map(|(i, _)| {
                                if i == 0 {
                                    None
                                } else {
                                    Some(NetworkableTransactionPool::Transaction(Transaction::default_test_tx()))
                                }
                            }).collect();
                            let _ = node_b.send_response_msg(*request_id, V2ResponseMessage::TxPoolFullTransactions(Ok(txs)));
                        }
                    }
                }

                tracing::info!("Node B Event: {:?}", node_b_event);
            }
        };
    }
}

#[tokio::test]
#[instrument]
async fn request_response_works_with_transactions() {
    let arbitrary_range = 2..6;
    request_response_works_with(RequestMessage::Transactions(arbitrary_range), None).await
}

#[tokio::test]
#[instrument]
async fn request_response_works_with_sealed_headers_range_inclusive() {
    let arbitrary_range = 2..6;
    request_response_works_with(RequestMessage::SealedHeaders(arbitrary_range), None)
        .await
}

#[tokio::test]
#[instrument]
async fn request_response_works_with_transactions_ids() {
    request_response_works_with(RequestMessage::TxPoolAllTransactionsIds, None).await
}

#[tokio::test]
#[instrument]
async fn request_response_works_with_full_transactions() {
    let tx_ids = (0..10)
        .map(|_| Transaction::default_test_tx().id(&ChainId::new(1)))
        .collect();
    request_response_works_with(RequestMessage::TxPoolFullTransactions(tx_ids), None)
        .await
}

/// We send a request for transactions, but it's responded by only headers
#[tokio::test]
#[instrument]
async fn invalid_response_type_is_detected() {
    let mut p2p_config = Config::default_initialized("invalid_response_type_is_detected");

    // Node A
    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    let (tx_test_end, mut rx_test_end) = mpsc::channel::<bool>(1);

    let mut request_sent = false;

    loop {
        tokio::select! {
            message_sent = rx_test_end.recv() => {
                // we received a signal to end the test
                assert!(message_sent.unwrap(), "Received incorrect or missing message");
                break;
            }
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::PeerInfoUpdated { peer_id, block_height: _ }) = node_a_event {
                    if node_a.peer_manager.get_peer_info(&peer_id).is_some() {
                        // 0. verifies that we've got at least a single peer address to request message from
                        if !request_sent {
                            request_sent = true;

                            let (tx_orchestrator, rx_orchestrator) = oneshot::channel();
                            assert!(node_a.send_request_msg(None, RequestMessage::Transactions(0..2), ResponseSender::Transactions(tx_orchestrator)).is_ok());
                            let tx_test_end = tx_test_end.clone();

                            tokio::spawn(async move {
                                let response_message = rx_orchestrator.await;

                                if let Ok(response) = response_message {
                                    match response {
                                        Ok((_, Ok(_))) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Request succeeded unexpectedly");
                                        },
                                        Ok((_, Err(ResponseError::TypeMismatch))) => {
                                            // Got Invalid Response Type as expected, so end test
                                            let _ = tx_test_end.send(true).await;
                                        },
                                        Ok((_, Err(err))) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Unexpected error in P2P communication: {:?}", err);
                                        },
                                        Err(e) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Error in P2P before sending message: {:?}", e);
                                        },
                                    }
                                } else {
                                    let _ = tx_test_end.send(false).await;
                                    panic!("Orchestrator failed to receive a message: {:?}", response_message);
                                }
                            });
                        }
                    }
                }

                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            node_b_event = node_b.next_event() => {
                // 2. Node B receives the RequestMessage from Node A initiated by the NetworkOrchestrator
                if let Some(FuelP2PEvent::InboundRequestMessage{ request_id, request_message: _ }) = &node_b_event {
                    let sealed_headers: Vec<_> = arbitrary_headers_for_range(1..3);
                    let _ = node_b.send_response_msg(*request_id, V2ResponseMessage::SealedHeaders(Ok(sealed_headers)));
                }

                tracing::info!("Node B Event: {:?}", node_b_event);
            }
        };
    }
}

#[tokio::test]
#[instrument]
async fn req_res_outbound_timeout_works() {
    let mut p2p_config = Config::default_initialized("req_res_outbound_timeout_works");

    // Node A
    // setup request timeout to 1ms in order for the Request to fail
    p2p_config.set_request_timeout = Duration::from_millis(1);

    let mut node_a = build_service_from_config(p2p_config.clone()).await;

    // Node B
    p2p_config.bootstrap_nodes = node_a.multiaddrs();
    p2p_config.set_request_timeout = Duration::from_secs(20);
    let mut node_b = build_service_from_config(p2p_config.clone()).await;

    let (tx_test_end, mut rx_test_end) = tokio::sync::mpsc::channel(1);

    // track the request sent in order to avoid duplicate sending
    let mut request_sent = false;

    loop {
        tokio::select! {
            node_a_event = node_a.next_event() => {
                if let Some(FuelP2PEvent::PeerInfoUpdated { peer_id, block_height: _ }) = node_a_event {
                    if node_a.peer_manager.get_peer_info(&peer_id).is_some() {
                        // 0. verifies that we've got at least a single peer address to request message from
                        if !request_sent {
                            request_sent = true;

                            // 1. Simulating Oneshot channel from the NetworkOrchestrator
                            let (tx_orchestrator, rx_orchestrator) = oneshot::channel();

                            // 2a. there should be ZERO pending outbound requests in the table
                            assert_eq!(node_a.outbound_requests_table.len(), 0);

                            // Request successfully sent
                            let requested_block_height = RequestMessage::SealedHeaders(0..0);
                            assert!(node_a.send_request_msg(None, requested_block_height, ResponseSender::SealedHeaders(tx_orchestrator)).is_ok());

                            // 2b. there should be ONE pending outbound requests in the table
                            assert_eq!(node_a.outbound_requests_table.len(), 1);

                            let tx_test_end = tx_test_end.clone();

                            tokio::spawn(async move {
                                // 3. Simulating NetworkOrchestrator receiving a Timeout Error Message!
                                let response_message = rx_orchestrator.await;
                                if let Ok(response) = response_message {
                                    match response {
                                        Ok((_, Ok(_))) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Request succeeded unexpectedly");
                                        },
                                        Ok((_, Err(ResponseError::P2P(_)))) => {
                                            // Got Invalid Response Type as expected, so end test
                                            let _ = tx_test_end.send(true).await;
                                        },
                                        Ok((_, Err(err))) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Unexpected error in P2P communication: {:?}", err);
                                        },
                                        Err(e) => {
                                            let _ = tx_test_end.send(false).await;
                                            panic!("Error in P2P before sending message: {:?}", e);
                                        },
                                    }
                                } else {
                                    let _ = tx_test_end.send(false).await;
                                    panic!("Orchestrator failed to receive a message: {:?}", response_message);
                                }
                            });
                        }
                    }
                }

                tracing::info!("Node A Event: {:?}", node_a_event);
            },
            recv = rx_test_end.recv() => {
                assert_eq!(recv, Some(true), "Test failed");
                // we received a signal to end the test
                // 4. there should be ZERO pending outbound requests in the table
                // after the Outbound Request Failed with Timeout
                assert_eq!(node_a.outbound_requests_table.len(), 0);
                break;
            },
            // will not receive the request at all
            node_b_event = node_b.next_event() => {
                tracing::info!("Node B Event: {:?}", node_b_event);
            }
        };
    }
}

#[tokio::test]
async fn gossipsub_peer_limit_works() {
    tokio::time::timeout(
        Duration::from_secs(5),
        gossipsub_broadcast(
            GossipsubBroadcastRequest::NewTx(Arc::new(Transaction::default_test_tx())),
            GossipsubMessageAcceptance::Accept,
            Some(1), // limit to 1 peer, therefore the function will timeout, as it will not be able to propagate the message
        ),
    )
    .await
    .expect_err("Should have timed out");
}

#[tokio::test]
async fn request_response_peer_limit_works() {
    let handle = tokio::spawn(async {
        let arbitrary_range = 2..6;

        tokio::time::timeout(
            Duration::from_secs(5),
            request_response_works_with(
                RequestMessage::Transactions(arbitrary_range),
                Some(0), // limit to 0 peers,
            ),
        )
        .await
    });

    let result = handle.await.expect("Should have completed");
    assert!(result.is_err());
}
