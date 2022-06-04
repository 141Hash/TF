# Especificação da estratégia para permitir execução de funções não-determinísticas e evitar execução em todas as réplicas

Uma vez que o problema proposto apresentava duas especificações bastante claras, tornou-se óbvio que iríamos ter que optar por realizar
uma replicação passiva de bases de dados.
Primeiro, para permitir a execução de funções não-determinísticas, uma vez que apenas a replicação passiva nos permite oferecer tal 
capacidade. Uma vez escolhida a estratégia, seria claro que não iria haver a execução em todas as réplicas, mas sim apenas
numa réplica primária que iria tratar de replicar as atualizações necessárias às restantes réplicas.

# Abordagem no código face ao problema em questão

Focando agora no código elaborado, seria necessário garantir que todas as réplicas iriam ter convergência de dados, ou seja,
seria necessário manter uma ordem de execução dos pedidos recebidos.
Para tal, recorremos à utilização do serviço LIN-TSO que nos oferecia um timestamp único e que seria capaz de ordenar os pedidos recebidos por parte dos clientes.

De salientar que cada réplica possui uma fila de espera em que a cada mensagem é associado um timestamp e existe a noção de um timestamp global, que serve como referência de comparação para ser possível determinar qual o pedido a ser tratado.

Assim, podemos fazer uma ordem cronológica de eventos que permitiu a implementação da replicação passiva de bases de dados.

1) Chega um pedido do cliente a uma réplica que será catologada como primária e irá executar esse mesmo pedido.
2) Aquando da chegada do pedido, a réplica envia um pedido ao LIN-TSO para associar um timestamp ao pedido.
3) Depois de ser catalogado com um timestamp, o pedido é colocada numa queue.
4) A réplica primária irá analisar a queue e irá executar aquele pedido que tiver o timestamp igual ao timestamp global.
5) Após a execução do pedido, a réplica irá enviar uma mensagem 'txn_replicated' para todas as réplicas, ela inclusive e o timestamp global será incrementado.
6) Cada réplica que receber a mensagem irá atualizar o seu estado, executando assim a replicação passiva.
7) Quando a réplica primária receber a própria mensagem 'txn_replicated' que enviou, ela irá responder ao cliente com o resultado da execução.