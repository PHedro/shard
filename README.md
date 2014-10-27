shard
=====

Para executar no terminal: ruby init.rb

Shard realizado em relação ao uuid do user, não foram consideradas técnicas para balanceamento dos servidores.

Ao finalizar o shard não foi removido o log original, então no final da operação,
teremos uma carga de disco 2x o tamanho da original, podendo ter picos de 3x durante o processo. 

Arquivo config_shard.json :
{
  "nodes": {
    "0": { ---> index do servidor
      "name": "Servidor1", ---> nome do servidor
      "path": "/server_1", ---> caminho do servidor ( no caso do exemplo caminho relativo da pasta do servidor ) 
      "files": ["/server_1/tmp/sample_file_more"] ---> caminhos dos arquivos de log a serem divididos
    }
  }
}

Os arquivos finais de cada user estarão na pasta /tmp/ do servidor escolhido para ele.
Para a decisão de para qual servidor cada user deve ser encaminhado pegamos o primeiro caracter do uuid do mesmo,
dividimos pelo número de nós do cluster e usamos o resto para identificar o servidor para o qual ele deve ser encaminhado.

A função shard_logs da classe Shard recebe como parametro array com isntâncias de shard representando cada nós 
do cluster. Ela realiza sua tarefa em 3 tempos distintos: 
1) cada instancia shard lê os logs de seu nós correspondente e escreve cada entrada em arquivos temporários 
nos servidores correspondentes ao usuário daquela entrada.

2) cada "shard" abre uma thread para cada pasta de arquivos temporários de usuários escolhidos para estarem em seu nó. Essa thread então faz merge desses arquivos em um só e ao final da tarefa deleta os arquivos temporários correspondentes ao usuário.

3) nesse passo são deletados todos os arquivos e pastas temporárias que por ventura ainda existam.
