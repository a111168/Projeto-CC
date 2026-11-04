import struct

MSG_PUBLISH = 1 
MSG_DELIVERY = 2

HEADER_FORMAT = "!BBIQHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def empacotar_publicacao(tipo, qos, seq_num, message_id, topico, conteudo):
    topico_bytes = topico.encode("utf-8")
    conteudo_bytes = conteudo.encode("utf-8")
    cabecalho = struct.pack(
        HEADER_FORMAT,
        tipo,
        qos,
        seq_num,
        message_id,
        len(topico_bytes),
        len(conteudo_bytes)
    )
    return cabecalho + topico_bytes + conteudo_bytes

def desempacotar_publicacao(dados):
    if len(dados) < HEADER_SIZE:
        raise ValueError("Cabeçalho EventCast incompleto")
    cabecalho = dados[:HEADER_SIZE] 
    tipo, qos, seq_num, message_id, len_topico, len_conteudo = struct.unpack(HEADER_FORMAT, cabecalho)

    tamanho_esperado = HEADER_SIZE + len_topico + len_conteudo
    if len(dados) != tamanho_esperado:
        raise ValueError("Tamanho da mensagem EventCast inválida")
    inicio_topico = HEADER_SIZE
    fim_topico = inicio_topico + len_topico
    topico = dados[inicio_topico:fim_topico].decode("utf-8")

    conteudo = dados[fim_topico:].decode("utf-8")
    return tipo, qos, seq_num, message_id, topico, conteudo
