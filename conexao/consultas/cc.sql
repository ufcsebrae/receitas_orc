SELECT DISTINCT
    A.CODCCUSTO,
    Nivel2.CODCCUSTO AS Nivel2_Cod,
    Nivel2.NOME       AS Nivel2_Nome,
    Nivel1.NOME       AS Nivel1_Nome,
    A.NOME            AS Nivel3_Nome

FROM CorporeRM.GCCUSTO A

-- Junta com Nível 1 (antes do primeiro ponto)
LEFT JOIN CorporeRM.GCCUSTO Nivel1
    ON Nivel1.CODCOLIGADA = A.CODCOLIGADA
   AND Nivel1.CODCCUSTO = LEFT(A.CODCCUSTO, CHARINDEX('.', A.CODCCUSTO + '.') - 1)

-- Junta com Nível 2 (até o segundo ponto)
LEFT JOIN CorporeRM.GCCUSTO Nivel2
    ON Nivel2.CODCOLIGADA = A.CODCOLIGADA
   AND Nivel2.CODCCUSTO = LEFT(A.CODCCUSTO, CHARINDEX('.', A.CODCCUSTO + '.', CHARINDEX('.', A.CODCCUSTO) + 1) - 1)

-- Apenas centros de custo com 3 níveis (exatamente 16 caracteres como "000001.000001.016")
WHERE LEN(A.CODCCUSTO) = 16
  AND A.ATIVO = 't'
