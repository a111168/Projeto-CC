import socket
import time

from protocols.subcontrol import empacotar_mensagem, receber_mensagem, MSG_CONNECT_PUB, MSG_CONNACK
from protocols.eventcast import empacotar_publicacao, MSG_PUBLISH
from log import registar_publicacao

def iniciar(process_id, config):
    broker_info = config.get('broker_control')
    if not broker_info:
        print("[Erro Publisher] broker_control não definido na configuração.")
        return

    broker_ip, broker_port_str = broker_info.split(':')
    broker_port = int(broker_port_str)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        print(f"[Publisher {process_id}] A ligar ao Broker em {broker_ip}:{broker_port}...")
        client_socket.connect((broker_ip, broker_port)) #ligação TCP com o broker
        
        # 1. Enviar mensagem de ligação (CONNECT_PUB)
        msg_connect = empacotar_mensagem(MSG_CONNECT_PUB, process_id)
        client_socket.sendall(msg_connect)
        
        # 2. Receber a resposta (CONNACK) com a porta de dados UDP
        tipo, payload = receber_mensagem(client_socket)
        if tipo == MSG_CONNACK:
            porta_udp_broker = int(payload)
            print(
                f"[Publisher {process_id}] Sessão estabelecida "
                f"Porta UDP do Broker: {porta_udp_broker}"
            )
        else:
            raise ValueError(f"Resposta inesperada do Broker: tipo {tipo}")

        publicacoes=config.get('publish', []) #lista de publicaçoes
        log_path = config['log'] #caminho do ficheiro para onde vão ser registadas as publicaçoes
        message_id = 0

        start_after = float(config['start_after'])
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
                mensagem = empacotar_publicacao(MSG_PUBLISH, qos, message_id, message_id, topico, conteudo)  #mensagem eventcast
                udp_socket.sendto(mensagem, (broker_ip, porta_udp_broker))  #enviar por UDP ao broker
                registar_publicacao(log_path, process_id, topico, message_id) #registar em log
                print(
                    f"[Publisher {process_id}] "
                    f"Publicação {message_id} enviada: "
                    f"{topico} = {conteudo}"
                )
                if i < count - 1:
                    time.sleep(intervalo)

    except Exception as e:
        print(f"[Erro Publisher] Falha na comunicação com o Broker: {e}")
    finally:
        client_socket.close()
        udp_socket.close()