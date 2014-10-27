require './shard'

shard_0 = Shard.new 0
shard_1 = Shard.new 1
shard_2 = Shard.new 2
shard_3 = Shard.new 3

Shard.shard_logs([shard_0, shard_1, shard_2, shard_3])