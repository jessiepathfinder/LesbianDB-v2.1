# LesbianDB v2.1: high-performance, ACID-compilant NoSQL database

## Getting started

You can get help on LesbianDB Server by running `LesbianDB.Server --help`.

Here's the list of valid arguments to pass to `LesbianDB.Server` at the time of writing.

```
  --listen                           Required. The HTTP websocket prefix to listen to (e.g
                                     https://lesbiandb-eu.scalegrid.com/c160d449395b5fbe70fcd18cef59264b/)

  --engine                           Required. The storage engine to use (yuri/leveldb/saskia)

  --persist-dir                      The directory used to store the leveldb/saski/purrmemalloc on-disk dictionary
                                     (required for leveldb/purrmemalloc, optional for saskia, have no effect for yuri)

  --binlog                           The path of the binlog used for persistance/enhanced durability.

  --soft-memory-limit                The soft limit to memory usage (in bytes)

  --yurimalloc.buckets               (Default: 65536) The number of YuriMalloc generation 1 buckets to create (only
                                     useful for yuri storage engine, or saskia storage engine without --persist-dir
                                     set).

  --yurimalloc.gen2buckets           (Default: 0) The number of YuriMalloc generation 2 buckets to create (only useful
                                     for yuri storage engine, or saskia storage engine without --persist-dir set, zero
                                     means YuriMalloc generation 2 disabled).

  --yurimalloc.gen2promotiondelay    (Default: 7200) The number of seconds to defer promotion of YuriMalloc data from
                                     generation 1 to generation 2 (only useful for yuri storage engine, or saskia
                                     storage engine without --persist-dir set, and YuriMalloc generation 2 is enabled).

  --yuri.buckets                     (Default: 65536) The number of buckets to create (only used with Yuri storage
                                     engine).

  --saskia.zram                      (Default: false) Tells the Saskia storage engine to use zram instead of YuriMalloc
                                     for swapping cold data (no effect if persist-dir is specified or
                                     yuri/leveldb/purrmemalloc storage engine is used).

  --help                             Display this help screen.

  --version                          Display version information.
  
  ```

In our examples, we will start an ephemeral database server with `LesbianDB.Server --listen ws://localhost:12345 --engine saskia --saskia.zram`

### Reads, writes, and conditional writes

![image](https://user-images.githubusercontent.com/55774978/207563208-3b2dcfc7-9a83-486e-b322-c4dff5aba16d.png)

### Optimistic functions library
You may notice that the example above contains many optimization opportunilities and is a bit too complicated. To make life easier, we invented the optimistic functions library.

![image](https://user-images.githubusercontent.com/55774978/207564794-ad69902a-4b0e-4ec2-9e97-4c9dbed2ffba.png)

Here's an optimistic functions re-implementation of the first example. The optimistic functions library not only makes our life easier, but also performs many optimizations.

### Database persistence
A database is not very useful if it's ephemeral. Luckily, we have binlog, on-disk dictionary, and hybrid persistence.

#### Binlog persistence
Binlog persistence uses an append-only file that logs all database writes. It can withstand unexpected power interruption and crashes very well, but the database can take forever to load. Binlog persistence is supported by all storage engines.

`LesbianDB.Server --listen ws://localhost:12345 --engine saskia --binlog mylittledatabase.binlog`

#### On-disk dictionary persistence
On-disk dictionary persistence offers startup performance advantages at the cost of reduced durability against unexpected power interuption and crashes. It's not supported by the Yuri storage engine.

`LesbianDB.Server --listen ws://localhost:12345 --engine saskia --persist-dir mylittledatabase_persist`

#### Hybrid persistence
Hybrid persistence combines on-disk dictionary persistence with binlog persistence for fast startup and durability.

`LesbianDB.Server --listen ws://localhost:12345 --engine leveldb --persist-dir mylittledatabase_persist --binlog mylittledatabase.binlog`

#### Storage engine conversion
The binlog is useful if you want to switch storage engines. Hybrid and binlog persistence databases can be converted to a diffrent storage engine by simply loading the binlog with a new storage engine. Storage engine conversion for on-disk dictionary persistence and ephemeral databases are not supported yet.
