import socket
import sys
import threading
import time

from protocols.subcontrol import (
    desempacotar_connect_pub,
    desempacotar_connect_sub,
    desempacotar_subscribe,
    empacotar_connack,
    receber_mensagem,
    MSG_CONNECT_PUB,
    MSG_CONNECT_SUB,
    MSG_SUBSCRIBE,
)
from protocols.eventcast import empacotar_publicacao, desempacotar_publicacao,empacotar_ack,desempacotar_ack, MSG_PUBLISH, MSG_DELIVERY, MSG_ACK

ACK_TIMEOUT = 1.0

#Verifica se uma publicação corresponde a uma subscrição
def corresponde_topico(topico, filtro):
    if filtro.endswith("/*"):
        prefixo = filtro[:-1]
        return topico.startswith(prefixo)
    return topico == filtro

def retransmitir_pendentes(udp_socket, pendentes, pendentes_lock):
    while True:
        agora = time.monotonic()
        with pendentes_lock:
            for subscriber_id, entregas in pendentes.items():
                for pending_key, entrega in entregas.items():
                    if agora - entrega["ultimo_envio"] >= ACK_TIMEOUT:
                        udp_socket.sendto(entrega["mensagem"], entrega["destino"])
                        entrega["ultimo_envio"] = agora
                        print(
                            f"[Broker] Publicação {pending_key[1]} não está confirmada "
                            f"pelo subscriber {subscriber_id} após {ACK_TIMEOUT} segundos."
                            f"Possível perda de pacote ou ACK. A retransmitir..."
                        )
        time.sleep(0.1)

def evict_subscribers(subscribers, pendentes, pendentes_lock, evict_after):
    while True:
        agora = time.monotonic()
        sockets_para_fechar = []
        with pendentes_lock:
            for subscriber_id, entregas in list(pendentes.items()):
                if not entregas or subscriber_id not in subscribers:
                    continue

                info = subscribers[subscriber_id]
                instante_relevante = max(
                    info["ultima_confirmacao"],
                    info["inicio_pendente"],
                )
                if agora - instante_relevante > evict_after:
                    sockets_para_fechar.append((subscriber_id, info["client_sock"]))
                    del subscribers[subscriber_id]
                    del pendentes[subscriber_id]

        for subscriber_id, client_sock in sockets_para_fechar:
            print(f"[Broker] Subscriber {subscriber_id} expirou; sessão removida.")
            client_sock.close()
        time.sleep(0.1)

def receber_publicacao(udp_socket, subscribers, pendentes, pendentes_lock):
    print("[Broker] À espera de publicações...")
    while True:
        try:
            dados, endereco = udp_socket.recvfrom(4096)
            tipo = dados[0]
            if tipo == MSG_ACK:
                seq_num, message_id, topico = desempacotar_ack(dados)
                with pendentes_lock:
                    subscribers_snapshot = list(subscribers.items())
                for subscriber_id, info in subscribers_snapshot:  #identifica o subscriber que enviou o ACK e verifica se a publicação associada está pendente ou não
                    if (info["ip"], info["porta_udp"]) == endereco:
                        with pendentes_lock:
                            entregas = pendentes.get(subscriber_id, {})
                            entrega = entregas.get((topico, message_id))
                            if (subscribers.get(subscriber_id) is info
                                    and entrega is not None
                                    and entrega["seq_num"] == seq_num):
                                del entregas[(topico, message_id)]
                                subscribers[subscriber_id]["ultima_confirmacao"] = time.monotonic()
                                if not entregas:
                                    subscribers[subscriber_id]["inicio_pendente"] = None
                                confirmado = True
                            else:
                                confirmado = False
                        if confirmado:
                            print(f"[Broker] ACK confitmado do subscriber {subscriber_id}: message_id={message_id}, seq_num={seq_num}")
                        else:
                            print(f"[Broker] ACK inválido do subscriber {subscriber_id}: message_id={message_id}, seq_num={seq_num}")
                        break
                continue
            tipo, qos, seq_num, message_id, topico, conteudo = (desempacotar_publicacao(dados))  #Interpreta a publicação
            if tipo == MSG_PUBLISH:
                ack = empacotar_ack(seq_num, message_id, topico) #confirma a receção da publicação ao publisher
                udp_socket.sendto(ack, endereco)
                print(
                    f"[Broker] Publicacao recebida de {endereco}: "
                    f"Topico={topico}, "
                    f"Conteudo={conteudo}, "
                    f"QoS={qos}, "
                    f"MessageID={message_id}"
                )
                with pendentes_lock:
                    subscribers_snapshot = list(subscribers.items())
                for subscriber_id, info in subscribers_snapshot: #percorre os subscribers e verifica se as publicações correspondem às suas subscrições
                    for subscricao in info["subscricoes"]:
                        if corresponde_topico(topico, subscricao["topico"]):
                            mensagem = empacotar_publicacao(MSG_DELIVERY, min(qos, subscricao["qos"]), seq_num, message_id, topico, conteudo)
                            destino = (info["ip"], info["porta_udp"])
                            with pendentes_lock:
                                if subscribers.get(subscriber_id) is not info:
                                    break
                                if not pendentes.get(subscriber_id):
                                    pendentes[subscriber_id] = {}
                                    subscribers[subscriber_id]["inicio_pendente"] = time.monotonic()
                                pendentes[subscriber_id][(topico, message_id)] = {
                                    "seq_num": seq_num,
                                    "mensagem": mensagem,
                                    "destino": destino,
                                    "ultimo_envio": time.monotonic()
                                }
                            udp_socket.sendto(mensagem, destino)  #envia a mensagem do tipo delivery por UDP
                            print(
                                f"[Broker] Publicacao {message_id} "
                                f"enviada para {subscriber_id}"
                            )
                            break
        except OSError:
            break
        except ValueError as e:
            print(f"[Broker] Publicacao UDP inválida: {e}")


def atender_cliente(client_sock, addr, data_port, subscribers, pendentes, pendentes_lock):
    print(f"[Broker] Novo cliente ligado: {addr}")

    try:
        tipo_msg, payload_connect = receber_mensagem(client_sock) #recebe a primeira mensagem do cliente e verifica se é publisher ou subscriber
        if tipo_msg == MSG_CONNECT_SUB:  #se for subscriber lê o seu identificador, obtem a porta UDP onde vai receber mensagens e guarda-o
            process_id_cliente, porta_udp_sub = desempacotar_connect_sub(payload_connect)

            agora = time.monotonic()
            with pendentes_lock:
                subscribers[process_id_cliente] = {
                    "ip": addr[0],
                    "porta_udp": porta_udp_sub,
                    "subscricoes": [],
                    "client_sock": client_sock,
                    "ultima_confirmacao": agora,
                    "inicio_pendente": None,
                }
            print(
                f"[Broker] Subscriber {process_id_cliente} registado: "
                f"{addr[0]}:{porta_udp_sub}"
            )

        elif tipo_msg == MSG_CONNECT_PUB: #se for publisher lê o seu identificador e regista a ligação
            process_id_cliente = desempacotar_connect_pub(payload_connect)
            print(f"[Broker] Publisher {process_id_cliente} registado.")

        else:
            raise ValueError(f"Tipo de ligação desconhecida: {tipo_msg}")

        # broker comunica a porta UDP ao cliente
        msg_ack = empacotar_connack(int(data_port))
        client_sock.sendall(msg_ack)
        print(
            f"[Broker] Porta UDP {data_port} enviada "
            f"ao cliente {process_id_cliente}"
        )

        while True:
            tipo, payload = receber_mensagem(client_sock)

            if tipo == MSG_SUBSCRIBE and tipo_msg == MSG_CONNECT_SUB: #quando receber mensagem do subscriber guarda a informação necessária
                topico, qos = desempacotar_subscribe(payload)

                with pendentes_lock:
                    subscribers[process_id_cliente]["subscricoes"].append({"topico": topico,"qos": qos})
                print(
                    f"[Broker] Subscrição guardada: "
                    f"{process_id_cliente} -> {topico} (QoS {qos})"
                )

    except ConnectionError:
        print(f"[Broker] Cliente {addr} terminou a ligação.")

    except Exception as e:
        print(f"[Erro Broker - {addr}] {e}")

    finally:
        if ('tipo_msg' in locals() and tipo_msg == MSG_CONNECT_SUB and 'process_id_cliente' in locals()):
            with pendentes_lock:
                if (process_id_cliente in subscribers
                        and subscribers[process_id_cliente]["client_sock"] is client_sock):
                    del subscribers[process_id_cliente]
                    pendentes.pop(process_id_cliente, None)
                    print(f"[Broker] Subscriber {process_id_cliente} removido da lista de subscritores.")
        client_sock.close()
        print(f"[Broker] Sessão TCP com {addr} terminada.")


def iniciar(process_id, config):
    inicio = time.monotonic()
    bind_ip = config.get('bind', '0.0.0.0')
    control_port = int(config.get('control_port', 0))
    data_port = str(config.get('data_port', '0'))
    duration = float(config['duration'])
    evict_after = float(config['evict_after'])

    subscribers = {}
    pendentes = {}  #guarda as publicações que falta receber ACK para confirmar
    pendentes_lock = threading.Lock()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #cria socket TCP
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #cria socket UDP
    udp_socket.bind((bind_ip, int(data_port)))
    print(
        f"[Broker {process_id}] EventCast a escutar em "
        f"{bind_ip}:{data_port} (UDP)"
    )
    # permite receber publicaçoes enquanto recebe tambem ligaçoes TCP
    thread_udp = threading.Thread(target=receber_publicacao, args=(udp_socket, subscribers, pendentes, pendentes_lock), daemon=True) 
    thread_udp.start()
    thread_retransmissao = threading.Thread(target=retransmitir_pendentes, args=(udp_socket, pendentes, pendentes_lock), daemon=True)
    thread_retransmissao.start()
    thread_eviction = threading.Thread(
        target=evict_subscribers,
        args=(subscribers, pendentes, pendentes_lock, evict_after),
        daemon=True,
    )
    thread_eviction.start()
    
    try:
        server_socket.bind((bind_ip, control_port))
        server_socket.listen(5)
        server_socket.settimeout(0.2)
        print(f"[Broker {process_id}] SubControl a escutar em {bind_ip}:{control_port} (TCP)...")
    except Exception as e:
        print(f"[Erro Broker] Falha ao iniciar socket TCP: {e}")
        sys.exit(1)

    print("[Broker] À espera de clientes...")

    try:
        while time.monotonic() -inicio < duration:
            try:
                cliente_sock, addr = server_socket.accept()
            
            except socket.timeout:
                continue
            #permite atender vários cliente ao mesmo tempo
            thread_cliente = threading.Thread(target=atender_cliente, args=(cliente_sock, addr, data_port, subscribers, pendentes, pendentes_lock),daemon=True)
            thread_cliente.start()
    except KeyboardInterrupt:
        print("/n[Broker] A encerrar")
    finally:
        server_socket.close()
        udp_socket.close()

    
