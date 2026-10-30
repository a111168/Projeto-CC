import socket
from protocols.subcontrol import empacotar_mensagem, desempacotar_cabecalho, HEADER_SIZE, MSG_CONNACK_SUB, MSG_SUBSCRIBE

def iniciar(process_id, config):
    broker_info = config.get('broker_control')
    if not broker_info:
        print("[Erro Subscriber] broker_control não definido na configuração")
        return
    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        print(f"[Subscriber {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port))

        # 1. Enviar mensagem de ligação (CONNECT_SUB) com o nosso ID
        msg_connect = empacotar_mensagem(MSG_CONNECT_SUB, process_id)
        client_socket.sendall(msg_connect)

        # 2. Receber a resposta (CONNACK) com a porta de dados
        header_bytes = client_socket.recv(HEADER_SIZE)
        tipo_msg, tamanho_payload = desempacotar_cabecalho(header_bytes)

        data_port_bytes = client_socket.recv(tamanho_payload)
        porta_udp_broker = int(data_port_bytes.decode('utf-8'))
        print(f"[Subscriber {process_id}] Sessão SubControl estabelecida com o Broker.Porta UDP do Broker: {porta_udp_broker}")

        subscricoes = config.get('subscribe', [])
        for sub in subscricoes:
            topico = sub.get('_main', '')
            qos = sub.get('qos', 1)

            #Formatar a string de payload, ex = "north/station7/humidity qos=2"
            payload_str = f"{topico} qos={qos}"
            msg_subscribe = empacotar_mensagem(MSG_SUBSCRIBE, payload_str)
            client_socket.sendall(msg_subscribe)
            print(f"[Subscriber {process_id}] Pedido de subscrição enviado: {payload_str}")
        
    except Exception as e:
        print(f"[Erro Subscriber] Falha na comunicação com o Broker: {e}")
    finally:
        client_socket.close()