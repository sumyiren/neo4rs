use tokio;
use neo4rs::{query, Graph, Node, Session};
use std::sync::Arc;
use ::futures::future::{FutureExt};

// let mut result = session.write_transaction(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
// while let Ok(Some(row)) = result.next().await {
//     let node: Node = row.get("n").unwrap();
//     let name: String = node.get("name").unwrap();
//     println!("{}", name);
// }

const DEFAULT_MAX_CONNECTIONS: usize = 16;

#[cfg(test)]
mod integration_tests {
    
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    async fn setup_session(max_connections: usize) -> Session {
        let uri = "127.0.0.1:7687";
        let user = "integration_test_user";
        let pass = "integration_test_pass";
        let graph =  Arc::new(Graph::new_with_max_connections(&uri, user, pass, max_connections).await.unwrap());
        let driver = graph.create_driver().await;
        let session = driver.create_session();
        session.execute(query("MATCH (n) DETACH DELETE n")).await.unwrap();
        session
    }

    #[tokio::test]
    async fn test_session_execute() {
        let session = setup_session(1).await;
        let _result = session.execute(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
    }

    #[tokio::test]
    async fn test_session_run() {
        let session = setup_session(1).await;
        let mut result = session.run(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
        while let Ok(Some(row)) = result.next().await {
            let node: Node = row.get("n").unwrap();
            let name: String = node.get("name").unwrap();
            assert_eq!(name, "apple".to_string())
        }
    }

    #[tokio::test]
    async fn test_session_run_consume() {
        let session = setup_session(1).await;
        {
            let result = session.run(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
            result.consume().await;
        }
        {
            let result = session.run(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
            result.consume().await;
        }
        {
            let mut result = session.run(query("MATCH (n: Person) RETURN n")).await.unwrap();
            let mut count = 0;
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("n").unwrap();
                let _name: String = node.get("name").unwrap();
                count += 1;
            }
            assert_eq!(count, 2)
        }
    }

    #[tokio::test]
    async fn test_write_transaction() {
        let mut session = setup_session(1).await;
        {
            let _result = session.write_transaction(
                |txn| async move {
                    // let mut result = txn.run(query("CREATE (n: Person {name:'apple-pie'}) RETURN n")).await.unwrap();
                    // txn.consume().await;
                    // txn.commit().await;
                    txn.execute(query("CREATE (n: Person {name:'apple'}) RETURN n")).await;
                    txn.execute(query("CREATE (n: Person {name:'apple'}) RETURN n")).await;
                    txn.execute(query("CREATE (n: Person {name:'apple'}) RETURN n")).await;
                    // tx.run(query("CREATE (n: Person {name:'apple'}) RETURN n"));
                    Ok(())
                }.boxed()).await;
        }
    }

    #[tokio::test]
    async fn test_write_transaction_with_error() {
        let mut session = setup_session(1).await;
        {
            let _result = session.write_transaction(
                |txn| async move {
                    let result = txn.execute(query("CRAT (n: Person {name:'apple'}) RETURN n")).await;
                    result
                }.boxed()).await;
        }
    }

    // #[tokio::test]
    // async fn test_write_transaction_with_run() {
    //     let mut session = setup_session(1).await;
    //     {
    //         let mut result = session.write_transaction(
    //             |txn| async move {
    //                 let mut result = txn.run(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
    //                 while let Ok(Some(row)) = result.next().await {
    //                     let node: Node = row.get("n").unwrap();
    //                     let name: String = node.get("name").unwrap();
    //                     assert_eq!(name, "apple".to_string())
    //                 }
    //             }.boxed()).await;
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_read_transaction_pool() {
    //     let session = setup_session(1).await;
    //     {
    //         let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
    //         while let Ok(Some(row)) = result.next().await {
    //             let node: Node = row.get("n").unwrap();
    //             let name: String = node.get("name").unwrap();
    //             println!("{}", name);
    //         }
    //     } // connection is released and returned to pool here
    //
    //     {
    //         let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
    //         while let Ok(Some(row)) = result.next().await {
    //             let node: Node = row.get("n").unwrap();
    //             let name: String = node.get("name").unwrap();
    //             println!("{}", name);
    //         }
    //     }
    // }

    #[tokio::test]
    async fn test_execute_transaction() {
        let session = setup_session(1).await;
        {
            let txn = session.begin_transaction().await.unwrap();
            txn.execute(query("CREATE (n: Person {name:'apple-pie'}) RETURN n")).await.unwrap();
            txn.commit().await;
        }
        {
            let mut result = session.run(query("MATCH (n: Person) RETURN n")).await.unwrap();
            let mut all_results: Vec<String> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("n").unwrap();
                let name: String = node.get("name").unwrap();
                all_results.push(name);
            }
            assert_eq!(all_results.len(), 1)
        }
    }

    // #[tokio::test]
    // async fn test_unfinished_transaction() {
    //     let session = setup_session(1).await;
    //     {
    //         let txn = session.begin_transaction().await.unwrap();
    //         let mut result = txn.run(query("CREATE (n: Person {name:'apple-pie'}) RETURN n")).await.unwrap();
    //     }
    //     {
    //         let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
    //         let mut all_results: Vec<String> = Vec::new();
    //         while let Ok(Some(row)) = result.next().await {
    //             let node: Node = row.get("n").unwrap();
    //             let name: String = node.get("name").unwrap();
    //             all_results.push(name);
    //         }
    //         assert_eq!(all_results.len(), 0)
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_commit_transaction() {
    //     let session = setup_session(1).await;
    //     {
    //         let txn = session.begin_transaction().await.unwrap();
    //         let mut result = txn.run(query("CREATE (n: Person {name:'apple-pie'}) RETURN n")).await.unwrap();
    //         txn.consume_and_commit().await;
    //     }
    //     {
    //         let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
    //         let mut all_results: Vec<String> = Vec::new();
    //         while let Ok(Some(row)) = result.next().await {
    //             let node: Node = row.get("n").unwrap();
    //             let name: String = node.get("name").unwrap();
    //             all_results.push(name);
    //         }
    //         assert_eq!(all_results.len(), 1)
    //     }
    // }
}








