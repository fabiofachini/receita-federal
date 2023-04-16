-- Construcao market share acif

CREATE TABLE ASSOCIADOS (
	CNPJ VARCHAR(15) PRIMARY KEY
	)
;

\COPY ASSOCIADOS FROM 'D:/ASSOCIADOS.csv' DELIMITER ';' CSV ENCODING 'WIN1252';

SELECT
	GEO.REGIONAL AS "REGIONAL_ACIF",
	COUNT(ESTABELECIMENTOS.CNPJ_BASICO || ESTABELECIMENTOS.CNPJ_ORDEM || ESTABELECIMENTOS.CNPJ_DV) AS "CNPJ",
	COUNT(ASSOCIADOS.CNPJ) AS "ASSOCIADOS"
	
	FROM ESTABELECIMENTOS
	
	LEFT JOIN ASSOCIADOS
	ON ESTABELECIMENTOS.CNPJ_BASICO || ESTABELECIMENTOS.CNPJ_ORDEM || ESTABELECIMENTOS.CNPJ_DV  = ASSOCIADOS.CNPJ

	LEFT JOIN GEO
	ON ESTABELECIMENTOS.CEP = GEO.CEP

	WHERE ESTABELECIMENTOS.SITUACAO_CADASTRAL = '02' AND ESTABELECIMENTOS.MUNICIPIO = '8105'
	
	GROUP BY "REGIONAL_ACIF"

--____________________________________________________________________________________

-- Construcao da tabela de associados

SELECT
	ASSOCIADOS.CNPJ AS "ASSOCIADOS",
	EMPRESAS.RAZAO_SOCIAL AS "RAZAO_SOCIAL",
	ESTABELECIMENTOS.NOME_FANTASIA AS "NOME_FANTASIA",
	CASE
		WHEN ESTABELECIMENTOS.MATRIZ_FILIAL = '1' THEN 'MATRIZ'
		WHEN ESTABELECIMENTOS.MATRIZ_FILIAL = '2' THEN 'FILIAL'
		ELSE 'NAO INFORMADO'
	END AS "MATRIZ_FILIAL",
	CASE
		WHEN SIMPLES.OPCAO_MEI = 'S' THEN 'MEI'
		WHEN EMPRESAS.PORTE = '00' THEN 'NAO INFORMADO'
		WHEN EMPRESAS.PORTE = '01' THEN 'MICRO EMPRESA'
		WHEN EMPRESAS.PORTE = '03' THEN 'EPP'
		WHEN EMPRESAS.PORTE = '05' THEN 'DEMAIS PORTES'
	ELSE 'NAO INFORMADO'
	END AS "PORTE",
    EMPRESAS.CAPITAL_SOCIAL AS "CAPITAL_SOCIAL",
	ESTABELECIMENTOS.UF AS "ESTADO",
	MUNICIPIO.NOME_MUNICIPIO AS "MUNICIPIO",
	CASE 
		WHEN GEO.BAIRRO IS NOT NULL THEN GEO.BAIRRO
		ELSE 'OUTRA CIDADE'
	END	AS "BAIRRO",
    ESTABELECIMENTOS.CEP AS "CEP",
	ESTABELECIMENTOS.TIPO_LOGRADOURO || ' ' || ESTABELECIMENTOS.LOGRADOURO || ', NUMERO ' || ESTABELECIMENTOS.NUMERO_LOGRADOURO AS "ENDERECO",
    ESTABELECIMENTOS.CNAE_PRINCIPAL AS "COD_CNAE",
    UPPER(CNAE.NOME_CNAE) AS "CNAE_PRINCIPAL",
	CASE
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 322199 THEN 'AGRICULTURA'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 500301 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 3900500 THEN 'INDUSTRIA'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4110700 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 4399199 THEN 'CONSTRUCAO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4511101 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 4789099 THEN 'COMERCIO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4911600 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 8888887 THEN 'SERVICOS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 8888889 THEN 'SERVICOS'
		ELSE 'NAO_INFORMADO'
	END AS "SETOR",
	CASE
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 322199 THEN 'AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 500301 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 990403 THEN 'INDÚSTRIAS EXTRATIVAS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 1011201 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 3329599 THEN 'INDÚSTRIAS DE TRANSFORMAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 3511501 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 3530100 THEN 'ELETRICIDADE E GÁS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 3600601 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 3900500 THEN 'ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4110700 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 4399199 THEN 'CONSTRUÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4511101 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 4789099 THEN 'COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 4911600 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 5320202 THEN 'TRANSPORTE, ARMAZENAGEM E CORREIO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 5510801 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 5620104 THEN 'ALOJAMENTO E ALIMENTAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 5811500 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 6399200 THEN 'INFORMAÇÃO E COMUNICAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 6410700 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 6630400 THEN 'ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 6810201 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 6822600 THEN 'ATIVIDADES IMOBILIÁRIAS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 6911701 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 7500100 THEN 'ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 7711000 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 8299799 THEN 'ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 8411600 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 8430200 THEN 'ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 8511200 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 8599699 THEN 'EDUCAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 8610101 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 8800600 THEN 'SAÚDE HUMANA E SERVIÇOS SOCIAIS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 9001901 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 9329899 THEN 'ARTES, CULTURA, ESPORTE E RECREAÇÃO'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 9411100 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 9609299 THEN 'OUTRAS ATIVIDADES DE SERVIÇOS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 9700500 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 9700500 THEN 'SERVIÇOS DOMÉSTICOS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) >= 9900800 AND CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) <= 9900800 THEN 'ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS'
		WHEN CAST(ESTABELECIMENTOS.CNAE_PRINCIPAL AS INT) = 8888888  THEN 'NAO INFORMADO'
		ELSE 'NAO INFORMADO'
	END AS "CLASSE",
	CASE
		WHEN ESTABELECIMENTOS.SITUACAO_CADASTRAL = '01' THEN 'NULA'
		WHEN ESTABELECIMENTOS.SITUACAO_CADASTRAL = '02' THEN 'ATIVA'
		WHEN ESTABELECIMENTOS.SITUACAO_CADASTRAL = '03' THEN 'SUSPENSA'
		WHEN ESTABELECIMENTOS.SITUACAO_CADASTRAL = '04' THEN 'INAPTA'
		WHEN ESTABELECIMENTOS.SITUACAO_CADASTRAL = '08' THEN 'BAIXADA'
		ELSE 'NAO INFORMADO'
	END AS "SITUACAO_CADASTRAL",
    ESTABELECIMENTOS.DDD1 || ESTABELECIMENTOS.TELEFONE1 AS "TELEFONE_1_RF",
    ESTABELECIMENTOS.DDD2 || ESTABELECIMENTOS.TELEFONE2 AS "TELEFONE_2_RF",
    ESTABELECIMENTOS.EMAIL AS "EMAIL_RF",
	CASE 
		WHEN GEO.DISTRITO_PMF IS NOT NULL THEN GEO.DISTRITO_PMF
		ELSE 'OUTRA CIDADE'
	END	AS "DISTRITO",
	CASE 
		WHEN GEO.REGIONAL IS NOT NULL THEN GEO.REGIONAL
		ELSE 'OUTRA CIDADE'
	END	AS "REGIONAL_ACIF"

	FROM ESTABELECIMENTOS
		
	LEFT JOIN EMPRESAS
	ON ESTABELECIMENTOS.CNPJ_BASICO = EMPRESAS.CNPJ_BASICO

	LEFT JOIN SIMPLES
	ON ESTABELECIMENTOS.CNPJ_BASICO = SIMPLES.CNPJ_BASICO

	LEFT JOIN CNAE
	ON ESTABELECIMENTOS.CNAE_PRINCIPAL = CNAE.CODIGO_CNAE

	LEFT JOIN MUNICIPIO
	ON ESTABELECIMENTOS.MUNICIPIO = MUNICIPIO.CODIGO_MUNICIPIO

	LEFT JOIN GEO
	ON ESTABELECIMENTOS.CEP = GEO.CEP
	
	RIGHT JOIN ASSOCIADOS
	ON ESTABELECIMENTOS.CNPJ_BASICO || ESTABELECIMENTOS.CNPJ_ORDEM || ESTABELECIMENTOS.CNPJ_DV = ASSOCIADOS.CNPJ
;