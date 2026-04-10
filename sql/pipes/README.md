# Snowpipe - Pipes de Ingestão Automática

## Configuração do Pipe de Exchange Rates

### Pré-requisitos

1. ✅ **SNS Topic criado** na AWS
   - Nome: `snowpipe-exchange-rates-notifications`
   - Tipo: Standard
   - **Copiar o ARN do tópico**

2. ✅ **Event Notification configurado** no S3
   - Bucket: `snowflake-training-rd`
   - Prefix: `multicloud/exchange_rates/`
   - Suffix: `.json`
   - Destination: SNS Topic criado acima

3. ✅ **Stage configurado** no Snowflake
   - Verificar se o stage `@FORMACAO.PUBLIC.NORTH/exchange_rates/` existe
   - Ou ajustar o nome do stage no script do pipe

4. ✅ **File Format JSON** criado
   - `JSON_FORMAT` deve existir no schema

### Passos para Configurar

#### 1. Obter ARN do SNS Topic

No console AWS SNS:
- Abra o tópico `snowpipe-exchange-rates-notifications`
- Copie o ARN completo (ex: `arn:aws:sns:us-east-1:123456789012:snowpipe-exchange-rates-notifications`)

#### 2. Editar o Script do Pipe

Abra `pipe_exchange_rates.sql` e substitua:
- `arn:aws:sns:us-east-1:123456789012:snowpipe-exchange-rates-notifications` pelo ARN real do seu tópico
- Ajuste o stage se necessário: `@FORMACAO.PUBLIC.NORTH/exchange_rates/`

#### 3. Executar o Script no Snowflake

```sql
-- Execute o script pipe_exchange_rates.sql
-- O Snowflake criará automaticamente:
-- - A fila SQS
-- - A subscription do SNS na fila SQS
-- - Todas as permissões necessárias
```

#### 4. Configurar Permissões SNS (se necessário)

O Snowflake pode precisar de permissão para acessar o tópico SNS. 

**Via Console AWS:**
1. Abra o tópico SNS no console AWS
2. Vá em "Access policy"
3. Adicione uma política que permita o Snowflake acessar:
   - Account ID do Snowflake (fornecido na documentação do Snowflake)
   - Ação: `sns:Subscribe`, `sns:GetTopicAttributes`

**Ou via AWS CLI:**
```bash
aws sns add-permission \
  --topic-arn arn:aws:sns:us-east-1:123456789012:snowpipe-exchange-rates-notifications \
  --label snowflake-access \
  --aws-account-id <SNOWFLAKE_ACCOUNT_ID> \
  --action-name Subscribe GetTopicAttributes
```

#### 5. Verificar Status

```sql
-- Ver status do pipe
SELECT SYSTEM$PIPE_STATUS('FORMACAO.DEV.PIPE_EXCHANGE_RATES');

-- Ver histórico de carregamentos
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'BRONZE_EXCHANGE_RATES',
  START_TIME => DATEADD('hours', -24, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME = 'PIPE_EXCHANGE_RATES'
ORDER BY LAST_LOAD_TIME DESC;
```

### Testar o Pipe

1. Faça upload de um arquivo JSON no S3:
   ```bash
   # Via script Python
   python API/main.py
   
   # Ou manualmente via AWS CLI
   aws s3 cp arquivo.json s3://snowflake-training-rd/multicloud/exchange_rates/
   ```

2. Aguarde ~30-60 segundos

3. Verifique se os dados foram carregados:
   ```sql
   SELECT * FROM FORMACAO.DEV.bronze_exchange_rates 
   ORDER BY created_at DESC 
   LIMIT 10;
   ```

### Monitoramento

#### Ver status do pipe:
```sql
SELECT SYSTEM$PIPE_STATUS('FORMACAO.PUBLIC.PIPE_EXCHANGE_RATES');
```

#### Ver arquivos pendentes:
```sql
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START => DATEADD('hours', -24, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME = 'PIPE_EXCHANGE_RATES'
  AND TABLE_SCHEMA = 'DEV';
```

#### Ver erros:
```sql
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'BRONZE_EXCHANGE_RATES',
  START_TIME => DATEADD('hours', -24, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME = 'PIPE_EXCHANGE_RATES'
  AND TABLE_SCHEMA = 'DEV'
  AND STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC;
```

### Troubleshooting

#### Pipe não está carregando arquivos:

1. Verificar se o Event Notification está configurado no S3
2. Verificar se o ARN do SNS está correto no pipe
3. Verificar permissões do Snowflake no SNS Topic
4. Verificar se o stage está correto e acessível
5. Verificar logs de erro no Snowflake

#### Erro de permissão:

- Verificar se o Snowflake tem permissão para ler do S3
- Verificar se o Snowflake tem permissão para acessar o SNS Topic
- Verificar credenciais do stage

### Comandos Úteis

```sql
-- Pausar o pipe (se necessário)
ALTER PIPE FORMACAO.DEV.PIPE_EXCHANGE_RATES SET PIPE_EXECUTION_PAUSED = TRUE;

-- Retomar o pipe
ALTER PIPE FORMACAO.DEV.PIPE_EXCHANGE_RATES SET PIPE_EXECUTION_PAUSED = FALSE;

-- Ver definição do pipe
SHOW PIPES LIKE 'PIPE_EXCHANGE_RATES';

-- Recriar o pipe (se necessário)
-- Execute novamente o script pipe_exchange_rates.sql
```

