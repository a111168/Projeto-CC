import socket
from protocols.subcontrol import empacotar_mensagem, desempacotar_cabecalho, HEADER_SIZE, MSG_CONNECT_PUB, MSG_CONNACK

def iniciar(process_id, config):
    broker_info = config.get('broker_control')
    if not broker_info:
        print("[Erro Publisher] broker_control não definido na configuração.")
        return

    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        print(f"[Publisher {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port))
        
        # 1. Enviar mensagem de ligação (CONNECT_PUB)
        msg_connect = empacotar_mensagem(MSG_CONNECT_PUB, process_id)
        client_socket.sendall(msg_connect)
        
        # 2. Receber a resposta (CONNACK) com a porta de dados UDP
        header_bytes = client_socket.recv(HEADER_SIZE)
        tipo, tamanho = desempacotar_cabecalho(header_bytes)
        
        if tipo == MSG_CONNACK:
            data_port_bytes = client_socket.recv(tamanho)
            porta_udp_broker = int(data_port_bytes.decode('utf-8'))
            print(f"[Publisher {process_id}] Sessão estabelecida! Porta UDP do Broker: {porta_udp_broker}")
        else:
            print(f"[Erro] Resposta inesperada do Broker: tipo {tipo}")
            
    except Exception as e:
        print(f"[Erro Publisher] Falha na comunicação com o Broker: {e}")
    finally:
        # A ligação fecha após obter a porta, pois o envio de dados será por UDP (Fase 2)
        client_socket.close()