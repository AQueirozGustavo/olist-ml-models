-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Análise
-- MAGIC  - Analisando  < 06 meses  01-01-2018  menos 06 meses

-- COMMAND ----------

WITH tb_join_cte AS (

SELECT t2.*,
       t3.idVendedor
       
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido = t3.idPedido

WHERE dtPedido < '2018-01-01'
AND dtPedido >= ADD_MONTHS('2018-01-01',-6)
AND t3.idVendedor IS NOT NULL

),

tb_group_cte AS(

SELECT idVendedor,
       descTipoPagamento,
       COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
       SUM(vlPagamento) AS vlPedidoMeioPagamento
       
FROM  tb_join_cte

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento

)

SELECT idVendedor,

SUM(CASE WHEN descTipoPagamento = 'boleto' 
              THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' 
              THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' 
              THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' 
              THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,
              
SUM(CASE WHEN descTipoPagamento = 'boleto' 
              THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_boleto_pedido,
SUM(CASE WHEN descTipoPagamento = 'credit_card' 
              THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_credit_card_pedido,
SUM(CASE WHEN descTipoPagamento = 'voucher' 
              THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_voucher_pedido,
SUM(CASE WHEN descTipoPagamento = 'debit_card' 
              THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) AS pct_debit_card_pedido              
              

FROM tb_group_cte

GROUP BY 1



