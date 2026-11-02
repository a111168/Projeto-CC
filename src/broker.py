import socket
import sys

from protocols.subcontrol import empacotar_mensagem, receber_mensagem, MSG_CONNACK, MSG_SUBSCRIBE, MSG_CONNECT_SUB, MSG_CONNECT_PUB
from protocols.eventcast import desempacotar_publicacao, MSG_PUBLISH
def iniciar(process_id, config):
    bind_ip = config.get('bind', '0.0.0.0')
    control_port = int(config.get('control_port', 0))
    data_port = str(config.get('data_port', '0'))
    subscribers = {}

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind((bind_ip, int(data_port)))
    print(
        f"[Broker {process_id}] EventCast a escutar em "
        f"{bind_ip}:{data_port} (UDP)"
    )
    
    try:
        server_socket.bind((bind_ip, control_port))
        server_socket.listen(5)
        print(f"[Broker {process_id}] SubControl a escutar em {bind_ip}:{control_port} (TCP)...")
    except Exception as e:
        print(f"[Erro Broker] Falha ao iniciar socket TCP: {e}")
        sys.exit(1)

    print("[Broker] À espera de clientes...")
    
    try:
        # Ciclo sequencial
        while True:
            client_sock, addr = server_socket.accept()
            print(f"[Broker] Novo cliente ligado: {addr}")
            
            try:
                # 1. Receber o cabeçalho e o ID do cliente
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

                else:
                    raise ValueError(f"Tipo de ligação desconhecida: {tipo_msg}")
                
                # 2. Enviar a porta de dados UDP (CONNACK)
                msg_ack = empacotar_mensagem(MSG_CONNACK, data_port)
                client_sock.sendall(msg_ack)
                print(f"[Broker] Porta UDP ({data_port}) enviada ao cliente {process_id_cliente}.")
                
                # 3. Ler a subscrição inicial e terminar o atendimento se for um subscriber
                if tipo_msg == MSG_CONNECT_SUB:
                    tipo_sub, payload_sub = receber_mensagem(client_sock)
                    if tipo_sub == MSG_SUBSCRIBE:
                        partes = payload_sub.split()

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
                        print(f"[Broker] Subscribers: {subscribers}")
            except Exception as e:
                print(f"[Erro Broker - {addr}] Falha na comunicação: {e}")
            finally:
                client_sock.close()
                print(f"[Broker] Ligação com {addr} terminada. À espera do próximo...")
                
    except KeyboardInterrupt:
        print("\n[Broker] A encerrar...")
    finally:
        server_socket.close()
        udp_socket.close()