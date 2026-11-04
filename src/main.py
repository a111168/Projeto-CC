import argparse

from config import ler_configuracao 

def main():
    parser = argparse.ArgumentParser(description="Sistema de Vigilância e Alerta Ambiental")
    # Argumentos posicionais
    parser.add_argument("role", choices=["broker", "publisher", "subscriber"], help="Papel a desempenhar: broker, publisher ou subscriber")

    # Argumentos obrigatórios
    parser.add_argument("--id", required=True, help="Identificador do processo")
    parser.add_argument("--config", required=True, help="Caminho absoluto para o ficheiro de configuração")
    
    # Interpreta os argumentos recebidos da linha de comando
    args = parser.parse_args()

    print(f"[*] A arrancar processo {args.role.upper()} | ID: {args.id} | Config: {args.config}")

    # 1. Ler as configurações do ficheiro
    configuracoes = ler_configuracao(args.config)
    print(f"[*] Configurações lidas: {configuracoes}")

    # 2. Passar o ID do processo e as configurações para a função principal
    if args.role == "broker":
        print("-> Lógica do Broker a ser executada...")
        import broker
        broker.iniciar(args.id, configuracoes)

    elif args.role == "publisher":
        print("-> Lógica do Publisher a ser executada...")
        import publisher
        publisher.iniciar(args.id, configuracoes)

    elif args.role == "subscriber":
        print("-> Lógica do Subscriber a ser executada...")
        import subscriber
        subscriber.iniciar(args.id, configuracoes)


if __name__ == "__main__":
    main()