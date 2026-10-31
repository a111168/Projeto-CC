import socket
# Verifica se esta linha de importação está exata:
from protocols.subcontrol import empacotar_mensagem, desempacotar_cabecalho, HEADER_SIZE, MSG_CONNECT_SUB, MSG_SUBSCRIBE

def iniciar(process_id, config):
    broker_info = config.get('broker_control')
    if not broker_info:
        print("[Erro Subscriber] broker_control não definido na configuração.")
        return

    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        print(f"[Subscriber {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port))
        
        # 1. Enviar mensagem de ligação (CONNECT_SUB)
        msg_connect = empacotar_mensagem(MSG_CONNECT_SUB, process_id)
        client_socket.sendall(msg_connect)
        
        # 2. Receber a resposta (CONNACK) com a porta de dados UDP
        header_bytes = client_socket.recv(HEADER_SIZE)
        tipo, tamanho = desempacotar_cabecalho(header_bytes)
        
        data_port_bytes = client_socket.recv(tamanho)
        porta_udp_broker = int(data_port_bytes.decode('utf-8'))
        print(f"[Subscriber {process_id}] Sessão estabelecida! Porta UDP do Broker: {porta_udp_broker}")
        
        # 3. Enviar as subscrições iniciais (Lidas da configuração)
        subscricoes = config.get('subscribe', [])
        for sub in subscricoes:
            topico = sub.get('_main', '')
            qos = sub.get('qos', '1')
            
            payload_sub = f"{topico} qos={qos}"
            msg_sub = empacotar_mensagem(MSG_SUBSCRIBE, payload_sub)
            client_socket.sendall(msg_sub)
            print(f"[Subscriber {process_id}] Enviou subscrição: {payload_sub}")
            
    except Exception as e:
        print(f"[Erro Subscriber] Falha na comunicação com o Broker: {e}")
    finally:
        client_socket.close()