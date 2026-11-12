//! The memory store: notes in an embedded Grafeo graph.
//!
//! A memory is a `Memory` node holding its text, when it was written and its
//! topics; each topic is a `Topic` node the memory points at with an `ABOUT`
//! edge, so topics that come up together can be found from each other. A
//! BM25 text index over each memory's text and topics answers searches.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use grafeo::{GrafeoDB, Value};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// One stored memory.
#[derive(Debug, Clone, PartialEq)]
pub struct Memory {
    pub id: i64,
    pub text: String,
    pub topics: Vec<String>,
    /// Seconds since the Unix epoch.
    pub created: i64,
}

pub struct Store {
    db: GrafeoDB,
}

/// The database at a path, opened for each use and closed after it: an
/// open database holds its file locked, and every malachi session runs its
/// own server, so holding it would lock out all sessions but the first.
pub struct Db {
    path: PathBuf,
}

/// Whether opening failed because the database is open elsewhere: in
/// another process, or (in tests) another thread of this one.
fn locked(e: &Error) -> bool {
    let msg = e.to_string();
    msg.contains("cannot be locked") || msg.contains("already open in this process")
}

/// How long a use waits for another process to close the database.
const LOCK_WAIT: Duration = Duration::from_secs(5);

impl Db {
    /// Checks the database at path opens (creating it if need be).
    pub fn new(path: &Path) -> Result<Self, Error> {
        let db = Db { path: path.to_path_buf() };
        db.with(|_| Ok(()))?;
        Ok(db)
    }

    /// Runs f on the database, opened for it and closed after.
    pub fn with<T>(&self, f: impl FnOnce(&Store) -> Result<T, Error>) -> Result<T, Error> {
        let deadline = Instant::now() + LOCK_WAIT;
        let store = loop {
            match Store::open(&self.path) {
                Ok(s) => break s,
                Err(e) if locked(&e) && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(e) if locked(&e) => {
                    return Err(format!("the memory database is busy (another session is using it): {e}").into());
                }
                Err(e) => return Err(e),
            }
        };
        let out = f(&store);
        let closed = store.close();
        let out = out?;
        closed?;
        Ok(out)
    }
}

// Labels, edge type and property names.
const MEMORY: &str = "Memory";
const TOPIC: &str = "Topic";
const ABOUT: &str = "ABOUT";
const SEARCH: &str = "search"; // text and topics, the text index's property
const TOPIC_NAME: &str = "topic";

impl Store {
    /// Opens (or creates) the database at path.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let st = Store { db: GrafeoDB::open(path)? };
        st.ensure_index()?;
        Ok(st)
    }

    /// An in-memory store, for tests.
    #[cfg(test)]
    pub fn in_memory() -> Self {
        let st = Store { db: GrafeoDB::new_in_memory() };
        st.ensure_index().unwrap();
        st
    }

    /// Creates the text index if there is none.
    fn ensure_index(&self) -> Result<(), Error> {
        if self.db.text_index_options(MEMORY, SEARCH).is_none() {
            self.db.create_text_index(MEMORY, SEARCH)?;
        }
        Ok(())
    }

    pub fn close(&self) -> Result<(), Error> {
        Ok(self.db.close()?)
    }

    /// Stores text about topics and returns the new memory's id.
    pub fn remember(&self, text: &str, topics: &[String]) -> Result<i64, Error> {
        let text = text.trim();
        if text.is_empty() {
            return Err("text is empty".into());
        }
        let topics = normalize(topics);
        let joined = topics.join(", ");
        let node = self.db.create_node_with_props(
            &[MEMORY],
            [
                ("text", Value::from(text)),
                (SEARCH, Value::from(format!("{text}\n{joined}"))),
                ("topics", Value::from(joined.as_str())),
                ("created", Value::from(now())),
            ],
        )?;
        let id = node.as_u64() as i64;
        self.db.set_node_property(node, "mid", Value::from(id))?;
        for topic in &topics {
            let t = match self.db.find_nodes_by_property(TOPIC_NAME, &Value::from(topic.as_str()))?.first() {
                Some(&t) => t,
                None => self.db.create_node_with_props(&[TOPIC], [(TOPIC_NAME, Value::from(topic.as_str()))])?,
            };
            self.db.create_edge(node, t, ABOUT)?;
        }
        Ok(id)
    }

    /// The memories that best match query, best first.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<Memory>, Error> {
        let hits = self.db.text_search(MEMORY, SEARCH, query, limit, None)?;
        let mut out = Vec::new();
        for (node, _score) in hits {
            if let Some(m) = self.get(node.as_u64() as i64)? {
                out.push(m);
            }
        }
        Ok(out)
    }

    /// The memories about topic, newest first.
    pub fn about(&self, topic: &str, limit: usize) -> Result<Vec<Memory>, Error> {
        self.memories(
            &format!(
                "MATCH (m:{MEMORY})-[:{ABOUT}]->(t:{TOPIC}) WHERE t.{TOPIC_NAME} = $topic \
                 RETURN m.mid, m.text, m.topics, m.created ORDER BY m.created DESC LIMIT {limit}"
            ),
            params([("topic", Value::from(topic.trim().to_lowercase()))]),
        )
    }

    /// The newest memories.
    pub fn recent(&self, limit: usize) -> Result<Vec<Memory>, Error> {
        self.memories(
            &format!("MATCH (m:{MEMORY}) RETURN m.mid, m.text, m.topics, m.created ORDER BY m.created DESC LIMIT {limit}"),
            HashMap::new(),
        )
    }

    pub fn get(&self, id: i64) -> Result<Option<Memory>, Error> {
        Ok(self
            .memories(
                &format!("MATCH (m:{MEMORY}) WHERE m.mid = $id RETURN m.mid, m.text, m.topics, m.created"),
                params([("id", Value::from(id))]),
            )?
            .pop())
    }

    /// Deletes a memory and reports whether there was one.
    pub fn forget(&self, id: i64) -> Result<bool, Error> {
        if self.get(id)?.is_none() {
            return Ok(false);
        }
        self.db.execute_with_params(
            &format!("MATCH (m:{MEMORY}) WHERE m.mid = $id DETACH DELETE m"),
            params([("id", Value::from(id))]),
        )?;
        Ok(true)
    }

    /// Every topic with how many memories are about it, most first; or,
    /// given a topic, the topics that share memories with it.
    pub fn topics(&self, related_to: Option<&str>) -> Result<Vec<(String, i64)>, Error> {
        let res = match related_to {
            None => self.db.execute(&format!(
                "MATCH (m:{MEMORY})-[:{ABOUT}]->(t:{TOPIC}) RETURN t.{TOPIC_NAME} AS topic, count(m) AS n ORDER BY n DESC, topic"
            ))?,
            Some(topic) => self.db.execute_with_params(
                &format!(
                    "MATCH (a:{TOPIC})<-[:{ABOUT}]-(m:{MEMORY})-[:{ABOUT}]->(b:{TOPIC}) WHERE a.{TOPIC_NAME} = $topic \
                     RETURN b.{TOPIC_NAME} AS topic, count(m) AS n ORDER BY n DESC, topic"
                ),
                params([("topic", Value::from(topic.trim().to_lowercase()))]),
            )?,
        };
        Ok(res
            .rows()
            .iter()
            .filter_map(|r| Some((r.first()?.as_str()?.to_string(), r.get(1)?.as_int64()?)))
            .collect())
    }

    fn memories(&self, query: &str, params: HashMap<String, Value>) -> Result<Vec<Memory>, Error> {
        let res = self.db.execute_with_params(query, params)?;
        Ok(res
            .rows()
            .iter()
            .filter_map(|r| {
                let topics = r.get(2)?.as_str().unwrap_or("");
                Some(Memory {
                    id: r.first()?.as_int64()?,
                    text: r.get(1)?.as_str()?.to_string(),
                    topics: topics.split(", ").filter(|t| !t.is_empty()).map(String::from).collect(),
                    created: r.get(3)?.as_int64().unwrap_or(0),
                })
            })
            .collect())
    }
}

fn params<const N: usize>(kv: [(&str, Value); N]) -> HashMap<String, Value> {
    kv.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Topics are compared lowercased and trimmed, and kept once each.
fn normalize(topics: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in topics {
        let t = t.trim().to_lowercase();
        if !t.is_empty() && !out.contains(&t) {
            out.push(t);
        }
    }
    out
}

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn remember_recall_forget() {
        let st = Store::in_memory();
        let a = st.remember("Prefers table-driven tests in Go", &s(&["testing", "Go"])).unwrap();
        let b = st.remember("malachi is a Go coding agent harness", &s(&["malachi", "go"])).unwrap();
        st.remember("Uses jj? No: plain git", &s(&["git"])).unwrap();

        // Written after the index was created, and still found.
        assert_eq!(st.search("plain git", 5).unwrap().first().map(|m| m.text.as_str()), Some("Uses jj? No: plain git"));

        let hits = st.search("table driven tests", 5).unwrap();
        assert_eq!(hits.first().map(|m| m.id), Some(a), "{hits:?}");
        assert_eq!(hits[0].topics, s(&["testing", "go"]));

        let about: Vec<i64> = st.about("GO", 10).unwrap().iter().map(|m| m.id).collect();
        assert!(about.contains(&a) && about.contains(&b) && about.len() == 2, "{about:?}");

        assert_eq!(st.topics(None).unwrap()[0], ("go".to_string(), 2));
        let related = st.topics(Some("go")).unwrap();
        assert!(related.contains(&("malachi".to_string(), 1)) && related.contains(&("testing".to_string(), 1)), "{related:?}");

        assert_eq!(st.recent(10).unwrap().len(), 3);
        assert!(st.forget(a).unwrap());
        assert!(!st.forget(a).unwrap());
        assert!(st.get(a).unwrap().is_none());
        assert!(st.search("table driven", 5).unwrap().iter().all(|m| m.id != a));
        assert_eq!(st.about("go", 10).unwrap().len(), 1);
        assert!(st.remember("  ", &[]).is_err());
    }

    #[test]
    fn sessions_take_turns() {
        let dir = std::env::temp_dir().join(format!("malachi-memory-turns-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("memory.grafeo");
        let a = Db::new(&path).unwrap();
        let b = Db::new(&path).unwrap();
        // One session holds it a while; the other waits its turn.
        let holder = std::thread::spawn({
            let path = path.clone();
            move || {
                Db::new(&path).unwrap().with(|st| {
                    std::thread::sleep(Duration::from_millis(300));
                    st.remember("from session one", &[])
                })
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        b.with(|st| st.remember("from session two", &[])).unwrap();
        holder.join().unwrap().unwrap();
        let all = a.with(|st| st.recent(10)).unwrap();
        assert_eq!(all.len(), 2, "{all:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = std::env::temp_dir().join(format!("malachi-memory-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("memory.grafeo");
        let id = {
            let st = Store::open(&path).unwrap();
            let id = st.remember("survives a restart", &s(&["durability"])).unwrap();
            st.close().unwrap();
            id
        };
        let st = Store::open(&path).unwrap();
        assert_eq!(st.search("restart", 5).unwrap().first().map(|m| m.id), Some(id));
        assert_eq!(st.about("durability", 5).unwrap().len(), 1);
        st.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod timing {
    use super::*;

    #[test]
    #[ignore]
    fn open_close_cost() {
        let dir = std::env::temp_dir().join(format!("mm-timing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.grafeo");
        let st = Store::open(&path).unwrap();
        for i in 0..500 {
            st.remember(&format!("memory number {i} about something worth keeping"), &[format!("t{}", i % 20)]).unwrap();
        }
        st.close().unwrap();
        let t = std::time::Instant::now();
        for _ in 0..10 {
            let st = Store::open(&path).unwrap();
            st.search("worth keeping", 10).unwrap();
            st.close().unwrap();
        }
        eprintln!("open+search+close: {:?} each", t.elapsed() / 10);
        let t = std::time::Instant::now();
        for i in 0..10 {
            let st = Store::open(&path).unwrap();
            st.remember(&format!("extra {i}"), &["x".into()]).unwrap();
            st.close().unwrap();
        }
        eprintln!("open+remember+close: {:?} each", t.elapsed() / 10);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
