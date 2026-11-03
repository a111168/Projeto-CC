import socket
import time

from protocols.subcontrol import empacotar_mensagem, receber_mensagem, MSG_CONNECT_SUB, MSG_CONNACK, MSG_SUBSCRIBE
from protocols.eventcast import desempacotar_publicacao, MSG_DELIVERY
from log import registar_publicacao

def iniciar(process_id, config):
    inicio = time.monotonic()
    broker_info = config.get('broker_control')
    log_path = config['log']
    if not broker_info:
        print("[Erro Subscriber] broker_control não definido na configuração.")
        return

    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    # Criar socket UDP para receber as publicações
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(("0.0.0.0", 0))
    porta_udp_sub = udp_socket.getsockname()[1]
    print(f"[Subscriber {process_id}] Porta UDP local: {porta_udp_sub}")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        print(f"[Subscriber {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port))
        
        # 1. Enviar mensagem de ligação (CONNECT_SUB)
        msg_connect = empacotar_mensagem(MSG_CONNECT_SUB,f"{process_id} {porta_udp_sub}")
        client_socket.sendall(msg_connect)
        
        # 2. Receber a resposta (CONNACK) com a porta de dados UDP
        tipo, payload = receber_mensagem(client_socket)
        if tipo == MSG_CONNACK:
            porta_udp_broker = int(payload)
            print(
                f"[Subscriber {process_id}] Sessão estabelecida "
                f"Porta UDP do Broker: {porta_udp_broker}"
            )
        else:
            raise ValueError(f"Resposta inesperada do Broker: tipo {tipo}")
        
        # 3. Enviar as subscrições iniciais (Lidas da configuração)
        subscricoes = config.get('subscribe', [])
        for sub in subscricoes:
            topico = sub.get('_main', '')
            qos = sub.get('qos', '1')
            
            payload_sub = f"{topico} qos={qos}"
            msg_sub = empacotar_mensagem(MSG_SUBSCRIBE, payload_sub)
            client_socket.sendall(msg_sub)
            print(f"[Subscriber {process_id}] Enviou subscrição: {payload_sub}")

        duration = int(config.get('duration', 45))
        print(f"[Subscriber {process_id}] À espera de publicações UDP...")
        udp_socket.settimeout(1.0)
        while True:
            tempo_restante = duration - (time.monotonic() - inicio)
            if tempo_restante <= 0:
                break
            udp_socket.settimeout(min(1.0, tempo_restante))
            try:
                dados, endereco = udp_socket.recvfrom(65535)
                tipo, qos, seq_num, message_id, topico, conteudo = (desempacotar_publicacao(dados))
                if tipo == MSG_DELIVERY:
                    print(
                        f"[Subscriber {process_id}] Publicação recebida:\n"
                        f"  Topico: {topico}\n"
                        f"  Conteudo: {conteudo}\n"
                        f"  QoS: {qos}\n"
                        f"  MessageID: {message_id}\n"
                    )
                    registar_publicacao(log_path, process_id, topico, message_id)
            except socket.timeout:
                continue
            except (ValueError, UnicodeDecodeError) as e:
                print(f"[Subscriber {process_id}] Mesnsagem inválida: {e}")
        print(f"[Subscriber {process_id}] Tempo terminado")
            
    except Exception as e:
        print(f"[Erro Subscriber] Falha na comunicação com o Broker: {e}")
    finally:
        client_socket.close()
        udp_socket.close()