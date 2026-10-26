"""Testes da triagem de eventos de streaming (sem GCP). Rodar na raiz: python -m unittest discover -s tests -t ."""
import json
import unittest
from datetime import datetime, timedelta, timezone

from streaming.eventos import gerar_evento, gerar_evento_rejeitado
from streaming.validacao import CAMPOS_EVENTOS, classificar_mensagem, motivo_invalido


def msg(evento) -> str:
    return json.dumps(evento)


class TestMotivoInvalido(unittest.TestCase):
    def test_eventos_gerados_sao_validos(self):
        for _ in range(500):
            self.assertIsNone(motivo_invalido(gerar_evento()))

    def test_alfabetizado_derivado_do_corte_743(self):
        for _ in range(500):
            e = gerar_evento()
            self.assertEqual(e["alfabetizado"], "1" if e["proficiencia"] >= 743 else "0")

    def test_sem_id_municipio(self):
        self.assertIn("id_municipio", motivo_invalido(gerar_evento_rejeitado()))

    def test_municipio_fora_do_padrao_ibge(self):
        e = gerar_evento()
        e["id_municipio"] = "355030"
        self.assertIn("7 dígitos", motivo_invalido(e))

    def test_proficiencia_fora_da_faixa_e_nao_numerica(self):
        e = gerar_evento()
        e["proficiencia"] = 1200
        self.assertIn("faixa", motivo_invalido(e))
        e["proficiencia"] = "abc"
        self.assertIn("não numérico", motivo_invalido(e))

    def test_peso_zero(self):
        e = gerar_evento()
        e["peso_aluno"] = 0
        self.assertIn("peso", motivo_invalido(e))

    def test_alfabetizado_incoerente(self):
        e = gerar_evento()
        e["proficiencia"], e["alfabetizado"] = 600.0, "1"
        self.assertIn("inconsistente", motivo_invalido(e))

    def test_zero_como_texto_nao_conta_como_ausente(self):
        e = gerar_evento()
        e["proficiencia"], e["alfabetizado"] = 600.0, "0"
        self.assertIsNone(motivo_invalido(e))


class TestClassificarMensagem(unittest.TestCase):
    def test_valido_gera_linha_com_origem_e_latencia(self):
        e = gerar_evento()
        e["horario_producao"] = (datetime.now(timezone.utc) - timedelta(seconds=3)).isoformat()
        categoria, linha = classificar_mensagem(msg(e), set())
        self.assertEqual(categoria, "valido")
        self.assertEqual(list(linha), CAMPOS_EVENTOS)
        self.assertEqual(linha["origem"], "simulado")
        self.assertGreater(linha["latencia_segundos"], 0)

    def test_duplicado_entra_uma_vez(self):
        e, vistos = gerar_evento(), set()
        self.assertEqual(classificar_mensagem(msg(e), vistos)[0], "valido")
        self.assertEqual(classificar_mensagem(msg(e), vistos), ("duplicado", None))

    def test_invalido_vai_para_rejeitados_com_motivo_e_mensagem_original(self):
        e = gerar_evento_rejeitado()
        categoria, linha = classificar_mensagem(msg(e), set())
        self.assertEqual(categoria, "rejeitado")
        self.assertEqual(set(linha), {"id_evento", "horario_recebimento", "motivo_rejeicao", "mensagem_bruta"})
        self.assertEqual(linha["id_evento"], e["id_evento"])
        self.assertEqual(linha["mensagem_bruta"], msg(e))

    def test_invalido_reenviado_continua_rejeitado(self):
        e, vistos = gerar_evento_rejeitado(), set()
        self.assertEqual(classificar_mensagem(msg(e), vistos)[0], "rejeitado")
        self.assertEqual(classificar_mensagem(msg(e), vistos)[0], "rejeitado")

    def test_json_quebrado_nao_levanta_erro(self):
        categoria, linha = classificar_mensagem('{"id_evento": "x", ', set())
        self.assertEqual((categoria, linha["motivo_rejeicao"]), ("rejeitado", "JSON inválido"))

    def test_json_que_nao_e_objeto(self):
        self.assertEqual(classificar_mensagem("[1, 2]", set())[0], "rejeitado")

    def test_evento_sem_fuso_assume_utc(self):
        e = gerar_evento()
        e["horario_producao"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        categoria, linha = classificar_mensagem(msg(e), set())
        self.assertEqual(categoria, "valido")
        self.assertGreaterEqual(linha["latencia_segundos"], 0)


if __name__ == "__main__":
    unittest.main()
