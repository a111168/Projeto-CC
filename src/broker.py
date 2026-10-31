import socket
import sys

from protocols.subcontrol import empacotar_mensagem, desempacotar_cabecalho, HEADER_SIZE, MSG_CONNACK, MSG_SUBSCRIBE

def iniciar(process_id, config):
    bind_ip = config.get('bind', '0.0.0.0')
    control_port = int(config.get('control_port', 0))
    data_port = str(config.get('data_port', '0'))

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
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
                header = client_sock.recv(HEADER_SIZE)
                if not header:
                    continue
                tipo_msg, tamanho = desempacotar_cabecalho(header)
                process_id_cliente = client_sock.recv(tamanho).decode('utf-8')
                print(f"[Broker] Recebido pedido de ligação do cliente: {process_id_cliente}")
                
                # 2. Enviar a porta de dados UDP (CONNACK)
                msg_ack = empacotar_mensagem(MSG_CONNACK, data_port)
                client_sock.sendall(msg_ack)
                print(f"[Broker] Porta UDP ({data_port}) enviada ao cliente {process_id_cliente}.")
                
                # 3. Ler a subscrição inicial e terminar o atendimento
                header_sub = client_sock.recv(HEADER_SIZE)
                if header_sub:
                    tipo_sub, tamanho_sub = desempacotar_cabecalho(header_sub)
                    payload_sub = client_sock.recv(tamanho_sub).decode('utf-8')
                    if tipo_sub == MSG_SUBSCRIBE:
                        print(f"[Broker] Recebeu subscrição de {process_id_cliente}: {payload_sub}")
                        
            except Exception as e:
                print(f"[Erro Broker - {addr}] Falha na comunicação: {e}")
            finally:
                client_sock.close()
                print(f"[Broker] Ligação com {addr} terminada. À espera do próximo...")
                
    except KeyboardInterrupt:
        print("\n[Broker] A encerrar...")
    finally:
        server_socket.close()