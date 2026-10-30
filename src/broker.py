import socket 
import sys
from protocols.subcontrol import empacotar_mensagem, desempacotar_cabecalho, HEADER_SIZE, MSG_CONNACK

def iniciar(process_id, config):
    #Extrair os valores lidos da configuração
    bind_id = config.get('bind', '0.0.0.0')
    control_port = int(config.get('control_port', 0))
    data_port = config.get('data_port', 0)

    #Criar a socket TCP (Plano de Controlo)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try: 
        server_socket.bind((bind_id, control_port))
        server_socket.listen(5)
        print(f"[Broker {process_id}] SubControl a escutar em {bind_ip}: {control_port} (TCP)...") 
    except Exception as e:
        print(f"[Erro Broker] Falha ao iniciar socket TCP: {e}")
        sys.exit(1)
    print("[Broker] À espera de clientes...")

    client_sock, addr = server_socket.accept()
    print(f"[Broker] Novo cliente conectado: {addr}")

    #1. Receber o pacote de CONNECT do cliente
    header = client_sock.recv(HEADER_SIZE)
    tipo_msg, tamanho = desempacotar_mensagem(header)
    process_id_cliente = client_sock.recv(tamanho).decode('utf-8')
    print(f"[Broker] Recebido pedido de conexão do cliente {process_id_cliente}")

    #2. Enviar a porta de dados UDP ao cliente (Mensagem CONNACK)
    msg_ack = empacotar_mensagem(MSG_NONNACK, data_port)
    client_sock.sendall(msg_ack)
    print(f"[Broker] Porta UDP ({data_port}) enviada ao cliente")

    client_sock.close()
    server_socket.close()
