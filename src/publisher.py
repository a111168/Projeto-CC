import socket
import time
import threading
import sys

ACK_TIMEOUT = 1.0
RETRANSMISSION_INTERVAL = 0.1   
MAX_ACK_WAIT = 10.0

from protocols.subcontrol import (
    desempacotar_connack,
    empacotar_connect_pub,
    receber_mensagem,
    MSG_CONNACK,
)
from protocols.eventcast import empacotar_publicacao, desempacotar_ack, MSG_PUBLISH
from log import registar_publicacao

def receber_acks(udp_socket, destino_esperado, pendentes, ack_events, lock, stop_event):
    udp_socket.settimeout(0.5)

    while not stop_event.is_set():
        try:
            dados, endereco = udp_socket.recvfrom(1024)
            if endereco != destino_esperado:
                print(f"[Publisher] ACK recebido de endereço inesperado: {endereco}")
                continue    

            seq_num, message_id, _ = desempacotar_ack(dados)

        except socket.timeout:
            continue

        except OSError:
            break

        except ValueError as erro:
            print(f"[Publisher] ACK inválido: {erro}")
            continue

        with lock:
            mensagem = pendentes.get(message_id)

            if mensagem is None:
                continue

            if mensagem["seq_num"] != seq_num:
                print(
                    f"[Publisher] ACK incompatível: "
                    f"message_id={message_id}, seq_num={seq_num}"
                )
                continue

            del pendentes[message_id]
            evento_ack = ack_events.get(message_id)
            if evento_ack is not None:
                evento_ack.set()

            print(
                f"[Publisher] ACK recebido: "
                f"message_id={message_id}, seq_num={seq_num}"
            )

def retransmitir_publicacoes(udp_socket,destino,pendentes,lock,stop_event,):
    while not stop_event.is_set():
        agora = time.monotonic()
        retransmissoes = []

        with lock:
            for mensagem in pendentes.values():
                if agora - mensagem["ultimo_envio"] >= ACK_TIMEOUT:
                    mensagem["ultimo_envio"] = agora
                    retransmissoes.append(mensagem["dados"])

        for dados in retransmissoes:
            try:
                udp_socket.sendto(dados, destino)
                print("[Publisher] Publicação retransmitida")
            except OSError as erro:
                print(f"[Publisher] Erro ao retransmitir publicação: {erro}")
                return

        stop_event.wait(RETRANSMISSION_INTERVAL)

def iniciar(process_id, config):
    broker_info = config.get('broker_control')
    if not broker_info:
        print("[Erro Publisher] broker_control não definido na configuração.")
        return

    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    pendentes = {}
    ack_events = {}
    pendentes_lock = threading.Lock()
    stop_acks = threading.Event()
    thread_acks = None
    thread_retransmissoes = None

    try:
        print(f"[Publisher {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port)) #ligação TCP com o broker
        
        # 1. Enviar mensagem de ligação (CONNECT_PUB)
        msg_connect = empacotar_connect_pub(process_id)
        client_socket.sendall(msg_connect)
        
        # 2. Receber a resposta (CONNACK) com a porta de dados UDP
        tipo, payload = receber_mensagem(client_socket)
        if tipo == MSG_CONNACK:
            porta_udp_broker = desempacotar_connack(payload)
            print(
                f"[Publisher {process_id}] Sessão estabelecida "
                f"Porta UDP do Broker: {porta_udp_broker}"
            )
        
        else:
            raise ValueError(f"Resposta inesperada do Broker: tipo {tipo}")

        destino_broker = (broker_ip, porta_udp_broker)

        thread_acks = threading.Thread(
            target=receber_acks,
            args=(udp_socket, destino_broker, pendentes, ack_events, pendentes_lock, stop_acks),
            daemon=True,
        )   
        thread_acks.start()


        thread_retransmissoes = threading.Thread(
            target=retransmitir_publicacoes,
            args=(udp_socket,destino_broker,pendentes,pendentes_lock,stop_acks,),
            daemon=True,
        )
        thread_retransmissoes.start()


        publicacoes=config.get('publish', []) #lista de publicaçoes
        log_path = config['log'] #caminho do ficheiro para onde vão ser registadas as publicaçoes
        message_id = 0

        start_after = float(config['start_after'])
        drain = float(config['drain'])
        if drain < 0:
            raise ValueError(f"Período drain inválido: {drain}")

        time.sleep(start_after)
        for publicacao in publicacoes:
            topico = publicacao['_main']
            qos = int(publicacao['qos'])
            count = int(publicacao['count'])
            rate = float(publicacao['rate'])
            if rate <= 0:
                raise ValueError(f"Taxa de publicação inválida: {rate}")

            intervalo = 1.0 / rate 

            for i in range(count):
                message_id += 1
                conteudo = f"{20 + i * 0.5:.1f}"
                seq_num = message_id

                mensagem = empacotar_publicacao(MSG_PUBLISH, qos, seq_num, message_id, topico, conteudo)  #mensagem eventcast
                evento_ack = threading.Event()
                with pendentes_lock:
                    pendentes[message_id] = {
                        "seq_num": seq_num,
                        "dados": mensagem,
                        "ultimo_envio": time.monotonic(),
                    }
                    ack_events[message_id] = evento_ack
                udp_socket.sendto(mensagem, destino_broker)  #enviar por UDP ao broker
                registar_publicacao(log_path, process_id, topico, message_id) #registar em log
                print(
                    f"[Publisher {process_id}] "
                    f"Publicação {message_id} enviada: "
                    f"{topico} = {conteudo}"
                )
                confirmado = evento_ack.wait(MAX_ACK_WAIT)
                if not confirmado:
                    confirmado = evento_ack.wait(drain)
                with pendentes_lock:
                    ack_events.pop(message_id, None)
                if not confirmado:
                    raise TimeoutError(f"Publicação {message_id} não confirmada após o drain")
                if i < count - 1:
                    time.sleep(intervalo)

        limite_drain = time.monotonic() + drain
        while time.monotonic() < limite_drain:
            with pendentes_lock:
                if not pendentes:
                    break
                
            time.sleep(0.1)
        with pendentes_lock:
            quantidade_pendentes = len(pendentes)
        if quantidade_pendentes > 0:
            raise TimeoutError(f"O período do drain terminou com {quantidade_pendentes} publicações sem ACK")

    except Exception as e:
        print(f"[Erro Publisher] Falha na comunicação com o Broker: {e}")
        sys.exit(1)
    finally:
        stop_acks.set()

        if thread_acks is not None:
            thread_acks.join(timeout=1)

        if thread_retransmissoes is not None:
            thread_retransmissoes.join(timeout=1)

        client_socket.close()
        udp_socket.close()