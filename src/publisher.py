import socket
from protocols.subcontrol import empacotar_mensagem, desempacotar_mensagem, HEADER_SIZE, MSG_CONNACK_PUB

def iniciar(process_id, config):
    broker_info = config.get ('broker_control')
    if not broker_info:
        print("[Erro Publisher] broker_control não definido na configuração")
        return
    broker_ip, broker_port = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        print(f"[Publisher {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port))

        #1. Enivar mensagem de ligação (CONNECT_PUB) com o nosso IP
        msg_connect = empacotar_mensagem(MSG_CONNACK_PUB, process_id)
        client_socket.sendall(msg_connect)

        #2. Receber a resposta (CONNACK) com a porta de dados
        header_byes = client_socket.recv(HEADER_SIZE)
        tipo, tamanho = desempacotar_mensagem(header_bytes)

        data_port_bytes = client_socket.recv(tamanho)
        porta_udp_broker = int(data_port_bytes.decode('utf-8'))

        print(f"[Publisher {process_id}] Sessão SubControl estabelecida com o Broker.")
        print(f"[Publisher {process_id}] O Broker vai usar a porta UDP: {porta_udp_broker} para o EventCast")

    except Exception as e:
        print(f"[Erro Publisher] Falha na comunicação com o Broker: {e}")

    finally:
        client_socket.close()
