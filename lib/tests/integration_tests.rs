use tokio;
use neo4rs::{query, Graph, Query, Node};
use std::sync::Arc;

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

    async fn setup_graph(max_connections: usize) -> Arc<Graph> {
        let uri = "127.0.0.1:7687";
        let user = "integration_test_user";
        let pass = "integration_test_pass";
        let graph =  Arc::new(Graph::new_with_max_connections(&uri, user, pass, max_connections).await.unwrap());
        graph
    }

    #[tokio::test]
    async fn test_session_run() {
        let graph = setup_graph(DEFAULT_MAX_CONNECTIONS).await;
        let driver = graph.create_driver().await;
        let session = driver.create_session();
        let res = session.run(query("MATCH (n) DETACH DELETE n")).await;
        match res {
            Ok(ok) => println!("a ok"),
            Err(notok) => println!("a error"),
        }
    }

    #[tokio::test]
    async fn test_write_transaction() {
        let graph = setup_graph(DEFAULT_MAX_CONNECTIONS).await;
        let driver = graph.create_driver().await;
        let session = driver.create_session();
        let mut result = session.write_transaction(query("CREATE (n: Person {name:'apple'}) RETURN n")).await.unwrap();
        while let Ok(Some(row)) = result.next().await {
            let node: Node = row.get("n").unwrap();
            let name: String = node.get("name").unwrap();
            println!("{}", name);
        }
    }

    #[tokio::test]
    async fn test_read_transaction_pool() {
        let graph = setup_graph(1).await;
        let driver = graph.create_driver().await;
        let session = driver.create_session();
        {
            let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("n").unwrap();
                let name: String = node.get("name").unwrap();
                println!("{}", name);
            }
        } // connection is released and returned to pool here

        {
            let mut result = session.read_transaction(query("MATCH (n: Person) RETURN n")).await.unwrap();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("n").unwrap();
                let name: String = node.get("name").unwrap();
                println!("{}", name);
            }
        }
    }
}