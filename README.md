shard
=====

Para executar no terminal: ruby init.rb

Shard realizado em relação ao uuid do user, não foram consideradas técnicas para balanceamento dos servidores.

Ao finalizar o shard não foi removido o log original, então no final da operação,
teremos uma carga de disco 2x o tamanho da original, podendo ter picos de 3x durante o processo. 
