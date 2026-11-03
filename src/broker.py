import socket
import sys
import threading
import time

from protocols.subcontrol import empacotar_mensagem, receber_mensagem, MSG_CONNACK, MSG_SUBSCRIBE, MSG_CONNECT_SUB, MSG_CONNECT_PUB
from protocols.eventcast import empacotar_publicacao, desempacotar_publicacao, MSG_PUBLISH, MSG_DELIVERY

def corresponde_topico(topico, filtro):
    if filtro.endswith("/*"):
        prefixo = filtro[:-1]
        return topico.startswith(prefixo)
    return topico == filtro


def receber_publicacao(udp_socket, subscribers):
    print("[Broker] À espera de publicações...")
    while True:
        try:
            dadods, endereco = udp_socket.recvfrom(4096)
            tipo, qos, seq_num, message_id, topico, conteudo = (desempacotar_publicacao(dadods))
            if tipo == MSG_PUBLISH:
                print(
                    f"[Broker] Publicacao recebida de {endereco}: "
                    f"Topico={topico}, "
                    f"Conteudo={conteudo}, "
                    f"QoS={qos}, "
                    f"MessageID={message_id}"
                )
                for subscriber_id, info in list(subscribers.items()):
                    for subscricao in info["subscricoes"]:
                        if corresponde_topico(topico, subscricao["topico"]):
                            mensagem = empacotar_publicacao(MSG_DELIVERY, min(qos, subscricao["qos"]), seq_num, message_id, topico, conteudo)
                            destino = (info["ip"], info["porta_udp"])
                            udp_socket.sendto(mensagem, destino)
                            print(
                                f"[Broker] Publicacao {message_id} "
                                f"enviada para {subscriber_id}"
                            )
                            break
        except OSError:
            break
        except ValueError as e:
            print(f"[Broker] Publicacao UDP inválida: {e}")


def atender_cliente(client_sock, addr, data_port, subscribers):

    print(f"[Broker] Novo cliente ligado: {addr}")

    try:
        # Receber a identificação do cliente
        tipo_msg, payload_connect = receber_mensagem(client_sock)

        if tipo_msg == MSG_CONNECT_SUB:

            partes = payload_connect.split()

            if len(partes) != 2:
                raise ValueError("CONNECT_SUB inválido")

            process_id_cliente = partes[0]
            porta_udp_sub = int(partes[1])

            if not 1 <= porta_udp_sub <= 65535:
                raise ValueError("Porta UDP inválida")

            subscribers[process_id_cliente] = {
                "ip": addr[0],
                "porta_udp": porta_udp_sub,
                "subscricoes": []
            }

            print(
                f"[Broker] Subscriber {process_id_cliente} registado: "
                f"{addr[0]}:{porta_udp_sub}"
            )

        elif tipo_msg == MSG_CONNECT_PUB:

            process_id_cliente = payload_connect

            print(f"[Broker] Publisher {process_id_cliente} registado.")

        else:
            raise ValueError(f"Tipo de ligação desconhecida: {tipo_msg}")

        # Enviar a porta UDP do broker
        msg_ack = empacotar_mensagem(MSG_CONNACK, data_port)
        client_sock.sendall(msg_ack)

        print(
            f"[Broker] Porta UDP {data_port} enviada "
            f"ao cliente {process_id_cliente}"
        )

        # Manter a sessão TCP aberta
        while True:
            tipo, payload = receber_mensagem(client_sock)

            if tipo == MSG_SUBSCRIBE and tipo_msg == MSG_CONNECT_SUB:

                partes = payload.split()

                if len(partes) != 2 or not partes[1].startswith("qos="):
                    raise ValueError("SUBSCRIBE inválido")

                topico = partes[0]
                qos = int(partes[1].split("=", 1)[1])

                if qos not in (1, 2):
                    raise ValueError("QoS inválido")

                subscribers[process_id_cliente]["subscricoes"].append({
                    "topico": topico,
                    "qos": qos
                })

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
            if process_id_cliente in subscribers:
                del subscribers[process_id_cliente]
                print(f"[Broker] Subscriber {process_id_cliente} removido da lista de subscritores.")
        client_sock.close()
        print(f"[Broker] Sessão TCP com {addr} terminada.")

def iniciar(process_id, config):
    inicio = time.monotonic()
    bind_ip = config.get('bind', '0.0.0.0')
    control_port = int(config.get('control_port', 0))
    data_port = str(config.get('data_port', '0'))
    duration = float(config['duration'])
    subscribers = {}

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind((bind_ip, int(data_port)))
    print(
        f"[Broker {process_id}] EventCast a escutar em "
        f"{bind_ip}:{data_port} (UDP)"
    )

    thread_udp = threading.Thread(target=receber_publicacao, args=(udp_socket, subscribers), daemon=True)
    thread_udp.start()
    
    try:
        server_socket.bind((bind_ip, control_port))
        server_socket.listen(5)
        server_socket.settimeout(1.0)
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
            
            thread_cliente = threading.Thread(
                target=atender_cliente,
                args=(cliente_sock, addr, data_port, subscribers),
                daemon=True
            )
            thread_cliente.start()
    except KeyboardInterrupt:
        print("/n[Broker] A encerrar")
    finally:
        server_socket.close()
        udp_socket.close()

    
