# Nifi example custom processor to route document by routing key on header
Library name: nifi-router.processor-nar

  Пример создания собственного процессора для использования в Apache NiFi.
Процессор принимает входящее сообщение и в зависимости от содержимого RoutingKey в заголовке сообщения, 
отправляет сообщение в очередь outputOrderQueue или outputInvoiceQueue.

Ссылки на использованную документацию:

https://community.cloudera.com/t5/Support-Questions/Is-possible-to-write-an-attribute-into-a-file-and-also-keep/td-p/184414

## Example
nifi-router.transformer-nar-1.12.1.nar

## Build
mvn clean install
