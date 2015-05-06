ZooKeeper task scheduling
=========================

[![Build Status](https://travis-ci.org/AeroNotix/tazk.svg)](https://travis-ci.org/AeroNotix/tazk)

This is totally untested and unsafe. Do not use.

About
=====

Tazk is an Erlang library intended to implement a simple way to run
 tasks for a group sequentially, safely and with an **at least once**
 processing guarantee.

Tazk uses the concept of a "task group" for which tasks need to be ran
sequentially.

Given the ZooKeeper tree such as:

```
  +--tazk_tasks
  |  +--group1
  |  +--group2
  |  +--group3
```

Tazk will execute any tasks for each group sequentially, but each
group will have it's tasks run in parallel with the other groups.

API
===

```erlang
{ok, _} = application:ensure_all_started(tazk), %% use a proper release!
TaskGroup = "group1",
TaskToRun = {io, format, ["~p~n", [hello_from_tazk]]},
ok = tazk:submit(TaskGroup, TaskToRun)
```

Once this task has been submitted any workers which are running will
attempt to lock the task group and begin processing any tasks that
have been assigned to that group.

Tazk will lock each group properly using ZooKeeper ephemeral
nodes. The tree would look like this:

```
  +--tazk_locks
  |  +--group1
```

Once the task has been completed, the results of the task will be
serialized into the results node:

```
  +--tazk_results
  |  +--group1task0000000004
  |  +--group1task0000000005
  |  +--group1task0000000006
  |  +--group1task0000000007
  |  +--group1task0000000008
  |  +--group1task0000000009
  |  +--group1task0000000020
```

Your application will be able to retrieve the potential result path
from tazk and then assign a ZooKeeper client process to watch until it
appears (not implemented, tbd).
