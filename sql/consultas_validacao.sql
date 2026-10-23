-- Consultas para validar a carga (banco "aula").

-- 1) Contratos e valor total por exercício
SELECT EXTRACT(YEAR FROM data_assinatura AT TIME ZONE 'America/Fortaleza') AS exercicio,
       COUNT(*)            AS contratos,
       SUM(valor_contrato) AS valor_total
FROM contratos
GROUP BY 1
ORDER BY 1;

-- 2) Teste do upsert: rode antes e depois de uma nova execução.
--    "total" não deve duplicar; "ultima_carga" deve avançar.
SELECT COUNT(*) AS total, MAX(data_carga) AS ultima_carga FROM contratos;

-- 3) Nenhum id repetido (deve retornar 0 linhas)
SELECT id, COUNT(*) FROM contratos GROUP BY id HAVING COUNT(*) > 1;

-- 4) Amostra dos contratos mais recentes
SELECT id, data_assinatura, valor_contrato, descricao_modalidade,
       descricao_situacao, descricao_nome_credor
FROM contratos
ORDER BY data_assinatura DESC
LIMIT 10;
