(function() {var implementors = {};
implementors["nebari"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/fs/struct.StdFile.html\" title=\"struct nebari::io::fs::StdFile\">StdFile</a>","synthetic":true,"types":["nebari::io::fs::StdFile"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/fs/struct.StdFileManager.html\" title=\"struct nebari::io::fs::StdFileManager\">StdFileManager</a>","synthetic":true,"types":["nebari::io::fs::StdFileManager"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/fs/struct.OpenStdFile.html\" title=\"struct nebari::io::fs::OpenStdFile\">OpenStdFile</a>","synthetic":true,"types":["nebari::io::fs::OpenStdFile"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/memory/struct.MemoryFile.html\" title=\"struct nebari::io::memory::MemoryFile\">MemoryFile</a>","synthetic":true,"types":["nebari::io::memory::MemoryFile"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/memory/struct.MemoryFileManager.html\" title=\"struct nebari::io::memory::MemoryFileManager\">MemoryFileManager</a>","synthetic":true,"types":["nebari::io::memory::MemoryFileManager"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/io/memory/struct.OpenMemoryFile.html\" title=\"struct nebari::io::memory::OpenMemoryFile\">OpenMemoryFile</a>","synthetic":true,"types":["nebari::io::memory::OpenMemoryFile"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/enum.Error.html\" title=\"enum nebari::Error\">Error</a>","synthetic":true,"types":["nebari::error::Error"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.Roots.html\" title=\"struct nebari::Roots\">Roots</a>&lt;F&gt;","synthetic":true,"types":["nebari::roots::Roots"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.ExecutingTransaction.html\" title=\"struct nebari::ExecutingTransaction\">ExecutingTransaction</a>&lt;F&gt;","synthetic":true,"types":["nebari::roots::ExecutingTransaction"]},{"text":"impl&lt;Root, F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.TransactionTree.html\" title=\"struct nebari::TransactionTree\">TransactionTree</a>&lt;Root, F&gt;","synthetic":true,"types":["nebari::roots::TransactionTree"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/enum.CompareAndSwapError.html\" title=\"enum nebari::CompareAndSwapError\">CompareAndSwapError</a>","synthetic":true,"types":["nebari::roots::CompareAndSwapError"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.Config.html\" title=\"struct nebari::Config\">Config</a>&lt;F&gt;","synthetic":true,"types":["nebari::roots::Config"]},{"text":"impl&lt;Root, F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.Tree.html\" title=\"struct nebari::Tree\">Tree</a>&lt;Root, F&gt;","synthetic":true,"types":["nebari::roots::Tree"]},{"text":"impl&lt;U&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/enum.AbortError.html\" title=\"enum nebari::AbortError\">AbortError</a>&lt;U&gt;","synthetic":true,"types":["nebari::roots::AbortError"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.ThreadPool.html\" title=\"struct nebari::ThreadPool\">ThreadPool</a>&lt;F&gt;","synthetic":true,"types":["nebari::roots::ThreadPool"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.TransactionLog.html\" title=\"struct nebari::transaction::TransactionLog\">TransactionLog</a>&lt;F&gt;","synthetic":true,"types":["nebari::transaction::log::TransactionLog"]},{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.LogEntry.html\" title=\"struct nebari::transaction::LogEntry\">LogEntry</a>&lt;'a&gt;","synthetic":true,"types":["nebari::transaction::log::LogEntry"]},{"text":"impl&lt;M&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.TransactionManager.html\" title=\"struct nebari::transaction::TransactionManager\">TransactionManager</a>&lt;M&gt;","synthetic":true,"types":["nebari::transaction::manager::TransactionManager"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.TransactionHandle.html\" title=\"struct nebari::transaction::TransactionHandle\">TransactionHandle</a>","synthetic":true,"types":["nebari::transaction::manager::TransactionHandle"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.TreeLock.html\" title=\"struct nebari::transaction::TreeLock\">TreeLock</a>","synthetic":true,"types":["nebari::transaction::manager::TreeLock"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.TreeLockHandle.html\" title=\"struct nebari::transaction::TreeLockHandle\">TreeLockHandle</a>","synthetic":true,"types":["nebari::transaction::manager::TreeLockHandle"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/transaction/struct.State.html\" title=\"struct nebari::transaction::State\">State</a>","synthetic":true,"types":["nebari::transaction::state::State"]},{"text":"impl&lt;T&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/tree/enum.KeyOperation.html\" title=\"enum nebari::tree::KeyOperation\">KeyOperation</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a>,&nbsp;</span>","synthetic":true,"types":["nebari::tree::btree_entry::KeyOperation"]},{"text":"impl&lt;'a, T&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.Modification.html\" title=\"struct nebari::tree::Modification\">Modification</a>&lt;'a, T&gt;","synthetic":true,"types":["nebari::tree::modify::Modification"]},{"text":"impl&lt;'a, T&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/tree/enum.Operation.html\" title=\"enum nebari::tree::Operation\">Operation</a>&lt;'a, T&gt;","synthetic":true,"types":["nebari::tree::modify::Operation"]},{"text":"impl&lt;'a, T&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.CompareSwap.html\" title=\"struct nebari::tree::CompareSwap\">CompareSwap</a>&lt;'a, T&gt;","synthetic":true,"types":["nebari::tree::modify::CompareSwap"]},{"text":"impl&lt;F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.TreeRoot.html\" title=\"struct nebari::tree::TreeRoot\">TreeRoot</a>&lt;F&gt;","synthetic":true,"types":["nebari::tree::root::TreeRoot"]},{"text":"impl&lt;Root&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.State.html\" title=\"struct nebari::tree::State\">State</a>&lt;Root&gt;","synthetic":true,"types":["nebari::tree::state::State"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.UnversionedTreeRoot.html\" title=\"struct nebari::tree::UnversionedTreeRoot\">UnversionedTreeRoot</a>","synthetic":true,"types":["nebari::tree::unversioned::UnversionedTreeRoot"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.VersionedTreeRoot.html\" title=\"struct nebari::tree::VersionedTreeRoot\">VersionedTreeRoot</a>","synthetic":true,"types":["nebari::tree::versioned::VersionedTreeRoot"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/tree/enum.PageHeader.html\" title=\"enum nebari::tree::PageHeader\">PageHeader</a>","synthetic":true,"types":["nebari::tree::PageHeader"]},{"text":"impl&lt;Root, F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.TreeFile.html\" title=\"struct nebari::tree::TreeFile\">TreeFile</a>&lt;Root, F&gt;","synthetic":true,"types":["nebari::tree::TreeFile"]},{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/tree/enum.KeyRange.html\" title=\"enum nebari::tree::KeyRange\">KeyRange</a>&lt;'a&gt;","synthetic":true,"types":["nebari::tree::KeyRange"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"nebari/tree/enum.KeyEvaluation.html\" title=\"enum nebari::tree::KeyEvaluation\">KeyEvaluation</a>","synthetic":true,"types":["nebari::tree::KeyEvaluation"]},{"text":"impl&lt;'a, F&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/tree/struct.PagedWriter.html\" title=\"struct nebari::tree::PagedWriter\">PagedWriter</a>&lt;'a, F&gt;","synthetic":true,"types":["nebari::tree::PagedWriter"]},{"text":"impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.Buffer.html\" title=\"struct nebari::Buffer\">Buffer</a>&lt;'a&gt;","synthetic":true,"types":["nebari::buffer::Buffer"]},{"text":"impl !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.ChunkCache.html\" title=\"struct nebari::ChunkCache\">ChunkCache</a>","synthetic":true,"types":["nebari::chunk_cache::ChunkCache"]},{"text":"impl&lt;M&gt; !<a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"nebari/struct.Context.html\" title=\"struct nebari::Context\">Context</a>&lt;M&gt;","synthetic":true,"types":["nebari::context::Context"]}];
implementors["xtask"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"enum\" href=\"xtask/enum.Commands.html\" title=\"enum xtask::Commands\">Commands</a>","synthetic":true,"types":["xtask::Commands"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.55.0/std/panic/trait.UnwindSafe.html\" title=\"trait std::panic::UnwindSafe\">UnwindSafe</a> for <a class=\"struct\" href=\"xtask/struct.CoverageConfig.html\" title=\"struct xtask::CoverageConfig\">CoverageConfig</a>","synthetic":true,"types":["xtask::CoverageConfig"]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()