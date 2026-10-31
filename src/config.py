import sys

def ler_configuracao(caminho_ficheiro):
    chaves_lista = ["publish", "subscribe"]  # Chaves que podem ter múltiplos valores

    config = {}

    try:
        with open(caminho_ficheiro, 'r') as f:
            for linha in f:
                linha = linha.split('#')[0].strip()  # Remove comentários e espaços em branco

                if not linha:
                    continue  # Ignora linhas vazias
                
                #Separar a chave do valor
                if '=' in linha:
                    chave, valor_bruto = linha.split('=', 1)
                    chave = chave.strip()
                    valor_bruto = valor_bruto.strip()

                    #Separar o valor por espaços para extrair o termo principal e as opções
                    partes = valor_bruto.split()

                    if len(partes) == 1:
                        #Valor simples (ex: bind = 10.0.3.20)
                        valor_final = partes[0]
                    else:
                        valor_final = {"_main": partes[0]}
                        for parte in partes[1:]:
                            if '=' in parte:
                                opt_chave, opt_valor = parte.split('=', 1)
                                valor_final[opt_chave] = opt_valor
                    
                    #Guardar no dicionário final dependendo se é chave única ou de lista
                    if chave in chaves_lista:
                        if chave not in config:
                            config[chave] = []
                        config[chave].append(valor_final)
                    else:
                        config[chave] = valor_final
    except FileNotFoundError:
        print(f"[Erro Config] Ficheiro de configuração não encontrado: {caminho_ficheiro}")
        sys.exit(1)
    return config