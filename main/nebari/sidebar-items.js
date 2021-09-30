initSidebarItems({"enum":[["AbortError","An error that could come from user code or Roots."],["CompareAndSwapError","An error returned from `compare_and_swap()`."],["Error","An error from `Roots`."]],"mod":[["transaction","ACID-compliant transaction log and manager."],["tree","Append-only B-Tree implementation"]],"struct":[["Buffer","A wrapper around a `Cow<'a, [u8]>` wrapper that implements Read, and has a convenience method to take a slice of bytes as another Buffer which shares a reference to the same underlying `Cow`."],["ChunkCache","A configurable cache that operates at the “chunk” level."],["Config","A database configuration used to open a database."],["Context","A shared environment for database operations."],["ExecutingTransaction","An executing transaction. While this exists, no other transactions can execute across the same trees as this transaction holds."],["Roots","A multi-tree transactional B-Tree database."],["StdFile","An open file that uses [`std::fs`]."],["TransactionTree","A tree that is modifiable during a transaction."],["Tree","A named collection of keys and values."]],"trait":[["FileManager","A manager that is responsible for controlling write access to a file."],["ManagedFile","A file that can be interacted with using async operations."],["Vault","A provider of encryption for blocks of data."]]});