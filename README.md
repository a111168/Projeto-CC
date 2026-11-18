# Projeto-CC
Um projeto para desenvolver o trabalho prático II de CC.

Alunos:
a111168-Luís Gonçalves
a111332-Ivo Gomes
a111169-Daniela Martins

# Como executar:
1. Abrir 3 terminais
2. Terminal 1: ./run.sh broker --id b1 --config configs/broker1.conf
3. Terminal 2: ./run.sh subscriber --id s1 --config configs/sub1.conf
4. Terminal 3: ./run.sh publisher --id p1 --config configs/pub1.conf

# Testar perdas manualmente no CORE

## Preparação

Iniciar a topologia do CORE e abrir terminais nos nós:

- `broker1`: executa o broker;
- `sub1`: executa o subscriber;
- `pub1`: executa o publisher.

Usar as configurações do projeto:

```text
configs/broker1.conf
configs/sub1.conf
configs/pub1.conf
```

## Iniciar o broker

No terminal do nó `broker1`:

```bash
cd /media/sf_Projeto-CC
./run.sh broker --id b1 --config configs/broker1.conf
```

O broker deve mostrar que está a escutar em:

```text
10.0.3.20:7310
```

## Iniciar o subscriber

No terminal do nó `sub1`:

```bash
cd /media/sf_Projeto-CC
./run.sh subscriber --id s1 --config configs/sub1.conf
```

## Iniciar o publisher

No terminal do nó `pub1`:

```bash
cd /media/sf_Projeto-CC
./run.sh publisher --id p1 --config configs/pub1.conf
```

A configuração pode usar, por exemplo:

```ini
publish = north/station7/temperature qos=1 count=10 rate=2
start_after = 2
drain = 10
```

## Capturar com Wireshark

No `broker1`, abrir o Wireshark na interface `eth0` e iniciar a captura antes dos processos.

Filtro de visualização:

```text
udp.port == 7311
```

Antes de aplicar perdas, guardar uma primeira captura sem perdas, por exemplo:

```text
sem-perdas.pcapng
```

Com 10 publicações e um subscriber, são esperados 40 datagramas UDP sem perdas:

```text
10 publicações publisher -> broker
10 ACKs broker -> publisher
10 entregas broker -> subscriber
10 ACKs subscriber -> broker
```

## Aplicar perdas

Executar `tc netem` no nó cuja saída se pretende perder.

### Perda publisher -> broker

No nó `pub1`:

```bash
sudo tc qdisc replace dev eth0 root netem loss 20%
```

Confirmar:

```bash
sudo tc qdisc show dev eth0
```

### Perda broker -> subscriber

No nó `broker1`:

```bash
sudo tc qdisc replace dev eth0 root netem loss 20%
```

Neste caso, o broker pode retransmitir `MSG_DELIVERY` ao subscriber.

### Perda dos ACKs do subscriber

No nó `sub1`:

```bash
sudo tc qdisc replace dev eth0 root netem loss 20%
```

Neste caso, o subscriber recebe e regista a entrega, mas o ACK pode perder-se. Como o sistema usa G1, a retransmissão pode ficar registada novamente no log do subscriber.

Para aplicar a mesma perda em vários sentidos, executar `netem` separadamente no nó correspondente.

Começar com 10% ou 20%. Repetir com 30% apenas depois de confirmar o funcionamento inicial.

## Observar retransmissões

No publisher:

```text
Publicação não confirmada pelo broker
A retransmitir
```

No broker:

```text
Publicação não está confirmada pelo subscriber
A retransmitir
```

No Wireshark:

```text
udp.port == 7311
```

Repetições do mesmo par:

```text
<topico> <message_id>
```

indicam retransmissões.

## Testar expulsão de subscriber

No terminal do `sub1`, obter o PID do subscriber:

```bash
ps -eo pid,cmd | grep '[s]ubscriber'
```

Suspender o processo:

```bash
sudo kill -STOP <PID>
```

Manter o processo suspenso mais tempo do que o valor `evict_after` do broker, por exemplo:

```ini
evict_after = 5
```

Resultado esperado:

- o broker continua a retransmitir;
- após `evict_after`, fecha a sessão TCP;
- remove o subscriber;
- limpa as entregas pendentes;
- não envia novas publicações ao subscriber.

Retomar, se necessário:

```bash
sudo kill -CONT <PID>
```

## Remover perdas

Executar em todos os nós onde foi aplicado `netem`:

```bash
sudo tc qdisc del dev eth0 root
```

Confirmar:

```bash
sudo tc qdisc show dev eth0
```

Não deve existir saída com `netem`.

## Recolher resultados

Logs:

```bash
wc -l /tmp/tp2/pub1.log
wc -l /tmp/tp2/sub1.log
tail -n 30 /tmp/tp2/sub1.log
```

Guardar no Wireshark:

```text
com-perdas.pcapng
```

Relatório do teste:

- percentagem de perda;
- nó e sentido em que a perda foi aplicada;
- número de publicações publicadas;
- número de entregas registadas;
- número de linhas repetidas no log G1;
- número de retransmissões;
- tempo até à expulsão, caso exista;
- número total de pacotes UDP mostrados no Wireshark.