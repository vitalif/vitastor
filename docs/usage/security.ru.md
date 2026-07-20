[Документация](../../README-ru.md#документация) → Использование → Безопасность в Vitastor

-----

[Read in English](security.en.md)

# Безопасность в Vitastor

- [Обзор](#обзор)
- [Быстрая настройка](#быстрая-настройка)
- Принципы работы
  - [Шифрование соединений с etcd (TLS)](#шифрование-соединений-с-etcd-tls)
  - [Шифрование соединений с OSD (AES-GCM)](#шифрование-соединений-с-osd-aes-gcm)
  - [Сквозное шифрование данных образов (AES-XTS)](#сквозное-шифрование-данных-образов-aes-xts)
  - [Аутентификация по сертификатам](#аутентификация-по-сертификатам)
  - [Пользователи и права доступа](#пользователи-и-права-доступа)
  - [Привилегии etcd](#привилегии-etcd)
- Ручная настройка
  - [Настройка шифрования соединений OSD](#настройка-шифрования-соединений-osd)
  - Варианты настройки etcd/Antietcd
    - [Mon со встроенным Antietcd](#mon-со-встроенным-antietcd)
    - [Mon в роли Etcd proxy](#mon-в-роли-etcd-proxy)
    - [Mon с отдельным Antietcd Proxy](#mon-с-отдельным-antietcd-proxy)
    - [Отдельный Antietcd без etcd](#отдельный-antietcd-без-etcd)
  - [Настройка Vault/OpenBao](#настройка-vaultopenbao)
    - [Пример настройки Vault](#пример-настройки-vault)
- Списки разрешённых операций
  - [Права доступа к данным etcd](#права-доступа-к-данным-etcd)
  - [Права доступа к данным OSD](#права-доступа-к-данным-osd)
  - [Права доступа к API](#права-доступа-к-api)
- [Производительность шифрования](#производительность-шифрования)

## Обзор

Начиная с версии 3.1.0, Vitastor предоставляет полную защиту данных: защиту слоя
управления (etcd), защиту слоя данных (OSD) и сквозное шифрование данных.

- Защита слоя управления:
  - Шифрование соединений с etcd (TLS)
  - Аутентификация по клиентским TLS (X.509) сертификатам
  - Контроль доступа клиентов к данным etcd
- Защита слоя данных:
  - Либо полное AES-GCM шифрование соединений с OSD (аналогично TLS, но быстрее)
  - Либо шифрование AES-GCM только заголовков команд с контрольными суммами данных с секретной "солью"
  - Аутентификация по клиентским TLS (X.509) сертификатам
  - Контроль доступа клиентов на стороне OSD
- Сквозное шифрование:
  - Данные шифруются AES-XTS на стороне клиента, кластер Vitastor не имеет доступа к открытым данным
  - Ключи AES-XTS могут храниться в etcd или во внешнем Vault/OpenBao

Все функции опциональны и в простейшем варианте настройки выключены. По умолчанию включены
только контрольные суммы данных на транспортном уровне ([proto_checksums](../config/security.ru.md#proto_checksums)=payload) для
поддерживающих их клиентов (>= 3.1.0). Для более старых клиентов по умолчанию разрешены
соединения без контрольных сумм данных ([force_proto_checksums](../config/security.ru.md#force_proto_checksums) пусто).

Для быстрой настройки перейдите к разделу [Быстрая настройка](#быстрая-настройка).

Описания всех параметров, связанных с безопасностью, читайте [здесь](../config/security.ru.md).

## Быстрая настройка

Для быстрой настройки используйте скрипт `/usr/lib/vitastor/mon/make-etcd`:

1. Зайдите на узел, на котором будет располагаться первый монитор и etcd.
2. Создайте там минимальный `/etc/vitastor/vitastor.conf` с параметрами etcd_address,
   osd_network и, если хотите включить привилегии - use_perms (обратите внимание на `https://`
   в адресах etcd):
   ```
   {
       "etcd_address": ["https://10.0.0.10:2379","https://10.0.0.11:2379","https://10.0.0.12:2379"],
       "osd_network": "10.0.0.0/24",
       "use_perms": true
   }
   ```
3. Запустите `/usr/lib/vitastor/mon/make-etcd` без параметров или с параметром `--antietcd-only`,
   если хотите инициализировать кластер только с Antietcd без etcd.
4. Скрипт сгенерирует все необходимые сертификаты и предложит скопировать их на остальные узлы
   мониторов. Для этого нужен root ssh доступ к этим узлам. Если он есть - соглашайтесь, скрипт
   автоматически скопирует конфигурацию и инициализирует все etcd и мониторы.
5. Если root ssh доступа с первого узла на остальные узлы мониторов нет - скопируйте на них конфигурацию
   вручную (vitastor.conf и все сертификаты и ключи, кроме ключей CA `*_ca.key`) и повторите на каждом
   узле вызов `/usr/lib/vitastor/mon/make-etcd`.

После этого можете переходить к инициализации OSD.

Если хотите разобраться в настройке подробнее, читайте далее разделы [Принципы работы](#принципы-работы)
и [Ручная настройка](#ручная-настройка).

## Принципы работы

### Шифрование соединений с etcd (TLS)

Варианты настройки:
- Без шифрования (http)
- С шифрованием (https)
- С шифрованием и аутентификацией по клиентским сертификатам. Используется либо тот
  же сертификат, что используется для аутентификации на стороне OSD (`cert`+`pkey` / `osd_cert`+`osd_pkey`),
  либо отдельно указанный сертификат (`etcd_client_cert`+`etcd_client_key`)

### Шифрование соединений с OSD (AES-GCM)

Варианты настройки:
- Без шифрования и без контрольных сумм: `proto_checksums=none`.
- Без шифрования, с контрольными суммами данных: `proto_checksums=payload` (можно не указывать,
  т.к. это значение по умолчанию). При этом контрольные суммы можно отключить на стороне
  клиента и можно использовать более старые версии клиента, не поддерживающие контрольные суммы.
  Если нужно запретить подключение клиентов без контрольных сумм, можно использовать опцию
  `force_proto_checksums=payload`.
- С шифрованием заголовков и контрольными суммами данных: активируется при установленных опциях
  `cert`, `pkey`, `osd_ca` на стороне клиента и `osd_cert`, `osd_pkey`, `osd_ca`, `client_ca`
  на стороне OSD, при `proto_checksums=payload`. При этом по умолчанию запрещается
  отключение контрольных сумм на уровне клиента, то есть используется `force_proto_checksums=payload`.
- С полным шифрованием всего трафика: аналогично прошлому варианту, но с `proto_checksums=gcm`.
  Клиенту при этом по умолчанию разрешается понизить уровень защиты до контрольных сумм, но
  это тоже можно запретить через `force_proto_checksums=gcm`. Данный вариант самый медленный и
  рекомендуется только для небезопасных (публичных) сетей. В том числе потому, что при использовании
  и полного шифрования трафика, и сквозного шифрования образов AES-XTS, данные шифруются дважды.

Для шифрования используется алгоритм AES-256-GCM и собственный упрощённый протокол согласования
ключей, полностью аналогичный TLS 1.3 ECDHE.

### Сквозное шифрование данных образов (AES-XTS)

Клиент Vitastor поддерживает шифрование данных каждого образа своим ключом. В этом случае на OSD
уходят уже зашифрованные данные и сами OSD не видят исходные данные клиента. При этом ключ можно
менять при клонировании/создании снимков образов. Например, можно сделать базовый образ ВМ
(условный Debian Linux) нешифрованным, но наследовать от него шифрованные образы клиентских ВМ.

Ключи шифрования образов могут храниться либо в etcd, либо во внешнем Vault. Во втором случае
в etcd хранятся только ID ключей, а Vitastor вообще не имеет доступа к данным образов. Для
использования Vault нужно создать образ с опцией `--enc_key vault:ID`, в конфигурации указать
опции vault_url и vault_ca, создать всем клиентам учётные записи в Vault и дать им доступ
к требуемым секретам v1.

Ещё раз повторимся, что если AES-XTS используется с полным шифрованием трафика (`proto_checksums=gcm`),
то данные образов шифруются дважды - сначала AES-XTS, а потом AES-GCM. Можете использовать,
только если вы совсем параноик :-).

### Аутентификация по сертификатам

При включённом шифровании клиенты, OSD и мониторы Vitastor аутентифицируются по сертификатам
как при соединениях с etcd (Antietcd), так и с OSD.

Для OSD и мониторов должны использоваться отдельные сертификаты - либо самоподписанные, либо
подписанные отдельными CA (`osd_ca` и `mon_ca`). При этом все OSD могут использовать один и
тот же сертификат и все мониторы тоже могут использовать один и тот же сертификат, так как
привилегии разных OSD или разных мониторов ничем не отличаются (теоретически можно было бы
сделать разграничение сертификатов OSD по пулам, но пока что такой необходимости не было).

Также сертификат монитора может быть вообще не нужен, если Antietcd встраивается в сам монитор.
В этом случае монитор и так имеет доступ ко всем данным etcd прямо в памяти.

### Пользователи и права доступа

При отключённом шифровании трафика Vitastor работает без контроля доступа, то есть,
любой клиент кластера имеет полный доступ как к слою управлению, так и к слою данных. Такой
вариант подходит для выделенных доверенных сетей хранения.

При включённом шифровании трафика OSD (хотя бы заголовков) есть возможность задействовать
контроль доступа, включив опцию `use_perms=true`. При включённой опции каждый пользователь
(и даже OSD и монитор) может выполнять только те операции, которые ему разрешены.

Каждый обычный пользователь, администратор, OSD или монитор должен иметь свой сертификат,
подписанный отдельным корневым сертификатом:
- Для обычных пользователей - `client_ca`. Common Name сертификата при этом должен быть равен
  имени пользователя. Обычные пользователи могут только читать и модифицировать явным образом
  разрешённые им образы и не могут менять общее состояние кластера.
- Для администраторов - `admin_ca`. Администраторы могут читать и модифицировать все образы,
  а также администрировать кластер: смотреть общую статистику и состояние, создавать и удалять
  OSD и так далее.
- Для OSD - `osd_ca`, для мониторов - `mon_ca` (если Antietcd запускаются отдельно от мониторов).
  OSD и мониторы тоже имеют свои наборы разрешённых операций.

Настройки привилегий для обычных пользователей хранятся в etcd и на данный момент состоят
всего из одного свойства в ключах `/vitastor/config/user/<имя>`:
- `groups` - Список имён групп, членом которых пользователь является.

У образов есть следующие свойства:
- Владелец (`owner`) - имя пользователя, которому разрешено и читать, и менять образ
- Группа владельцев (`owner_group`) - имя группы владельцев
- Группа читателей (`reader_group`) - имя группы пользователей, которым разрешено читать образ

И также есть свойство у пула:
- Группа создателей (`creator_group`) - имя группы пользователей, которым разрешено создавать образы в пуле

Перечень разрешённых операций с данными образов на стороне OSD смотрите в разделе
[Права доступа к данным OSD](#права-доступа-к-данным-osd).

### Привилегии etcd

Привилегии etcd реализуются через Antietcd во всех режимах работы.

Встроенные привилегии etcd не поддерживаются по причине их многочисленных неудобств:
- Аутентификация по сертификатам вообще не работает в REST интерфейсе etcd,
- Привилегии хранятся отдельно от k/v данных и не могут участвовать в транзакциях,
- Менять привилегии может только администратор (root),
- Нет поддержки фильтрации диапазонных ответов чтения по привилегиям.

Если используется etcd, то Antietcd выступает в роли фильтрующего прокси, при этом он
может быть встроен в монитор Vitastor или запущен отдельно. В этом случае etcd должен
разрешать входящие подключения только от Antietcd, а все остальные компоненты должны
соединяться с Antietcd.

Если Antietcd запускается в составе монитора Vitastor, то достаточно включить опцию
`use_perms=true` и задать нужные сертификаты. Если Antietcd запускается отдельно, то
привилегии нужно включать отдельно опциями Antietcd. Подробнее о настройке смотрите
раздел [Варианты настройки etcd/Antietcd](#варианты-настройки-etcdantietcd).

Перечень разрешённых операций с данными etcd смотрите в разделе
[Права доступа к данным etcd](#права-доступа-к-данным-etcd).

## Ручная настройка

### Настройка шифрования соединений OSD

Вам нужно 3 сертификата: один для OSD и по одному для подписи сертификатов клиентов и администраторов.
Для OSD можно использовать самоподписанный сертификат (osd_ca.crt) или отдельный сертификат (osd.crt),
подписанный доверенным сертификатом osd_ca.crt. Для клиентов нужно использовать отдельные
сертификаты, подписанные общим доверенным (client_ca.crt), для администраторов - отдельным доверенным
(admin_ca.crt).

В конфигурацию Vitastor на серверах OSD нужно добавить:
- use_perms: true
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- admin_ca: admin_ca.crt
- osd_cert: osd_ca.crt
- osd_pkey: osd_ca.key

На стороне клиентов:
- use_perms: true
- osd_ca: osd_ca.crt
- cert: client.crt
- pkey: client.key

### Варианты настройки etcd/Antietcd

Доступны следующие варианты настройки:

#### Mon со встроенным Antietcd

Самый простой вариант. Кроме сертификатов OSD вам нужен всего 1 сертификат для Antietcd (antietcd.crt).

Настройки Vitastor (`/etc/vitastor/vitastor.conf`):
- etcd_address: [ "http://mon1:2379", ... ] (адреса ваших мониторов с портом 2379)
- use_perms: true
- use_antietcd: true
- antietcd_cert: antietcd.crt
- antietcd_key: antietcd.key
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- admin_ca: admin_ca.crt

#### Mon в роли Etcd proxy

Если вы хотите включить привилегии, но остаться на etcd, можно задействовать режим etcd proxy.

Вам понадобится 2 отдельных сертификата: один для etcd (etcd.crt) и один для antietcd (antietcd.crt).
Клиентский порт etcd должен отличаться от стандартного 2379, например, можно выбрать 2381.

Настройки Vitastor:
- etcd_address: [ "http://mon1:2379", ... ] (адреса ваших мониторов с портом 2379)
- use_perms: true
- use_antietcd: true
- etcd_proxy:
  ```
  {
    "urls": [ "http://mon1:2381", ... ], // адреса ваших etcd с портом 2381
    "cert": "antietcd.crt",
    "key": "antietcd.key",
    "ca": "etcd.crt"
  }
  ```
- antietcd_cert: antietcd.crt
- antietcd_key: antietcd.key
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- admin_ca: admin_ca.crt

Опции командной строки etcd:
```
--advertise-client-urls=https://<АДРЕС>:2381 --listen-client-urls=https://<АДРЕС>:2381 \
--client-cert-auth --cert-file=etcd.crt --key-file=etcd.key --trusted-ca-file=antietcd.crt \
--peer-client-cert-auth --peer-cert-file=etcd.crt --peer-key-file=etcd.key --peer-trusted-ca-file=etcd.crt
```

#### Mon с отдельным Antietcd Proxy

Если в дополнение к предыдущему варианту вы хотите разгрузить Antietcd от задач монитора Vitastor,
можно запустить его отдельно.

Аналогично предыдущему варианту нужно 2 сертификата: один для etcd и один для antietcd, плюс понадобится
отдельный сертификат для монитора.

Настройки Vitastor:
- etcd_address: [ "http://mon1:2379", ... ] (адреса ваших мониторов с портом 2379)
- use_perms: true
- use_antietcd: false
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- admin_ca: admin_ca.crt
- mon_etcd_client_cert: mon_ca.crt
- mon_etcd_client_key: mon_ca.key

Опции командной строки Antietcd:
```
--port 2379 --cert /etc/vitastor/antietcd.crt --key /etc/vitastor/antietcd.key \
--client_cert_auth 1 --auth_filter /usr/lib/vitastor/mon/vitastor_auth_filter.js \
--ca /etc/vitastor/client_ca.crt --admin_ca /etc/vitastor/admin_ca.crt \
--osd_ca /etc/vitastor/osd_ca.crt --mon_ca /etc/vitastor/mon_ca.crt \
--etcd_proxy url1,url2,... --etcd_ca /etc/vitastor/etcd.crt \
--etcd_cert /etc/vitastor/antietcd.crt --etcd_key /etc/vitastor/antietcd.key
```

Опции командной строки etcd (не отличаются от предыдущего варианта):
```
--advertise-client-urls=https://<АДРЕС>:2381 --listen-client-urls=https://<АДРЕС>:2381 \
--client-cert-auth --cert-file=etcd.crt --key-file=etcd.key --trusted-ca-file=antietcd.crt \
--peer-client-cert-auth --peer-cert-file=etcd.crt --peer-key-file=etcd.key --peer-trusted-ca-file=etcd.crt
```

#### Отдельный Antietcd без etcd

Аналогично предыдущему варианту, но etcd и его сертификат не нужны:

Настройки Vitastor (не отличаются от предыдущего варианта):
- etcd_address: [ "http://mon1:2379", ... ] (адреса ваших мониторов с портом 2379)
- use_perms: true
- use_antietcd: false
- etcd_ca: antietcd.crt
- osd_ca: osd_ca.crt
- client_ca: client_ca.crt
- admin_ca: admin_ca.crt
- mon_etcd_client_cert: mon_ca.crt
- mon_etcd_client_key: mon_ca.key

Опции командной строки Antietcd:
```
--port 2379 --cert /etc/vitastor/antietcd.crt --key /etc/vitastor/antietcd.key \
--client_cert_auth 1 --auth_filter /usr/lib/vitastor/mon/vitastor_auth_filter.js \
--ca /etc/vitastor/client_ca.crt --admin_ca /etc/vitastor/admin_ca.crt \
--osd_ca /etc/vitastor/osd_ca.crt --mon_ca /etc/vitastor/mon_ca.crt \
--persist_filter /usr/lib/vitastor/mon/vitastor_persist_filter.js
```

### Настройка Vault/OpenBao

Для использования Vault каждому клиенту, который будет получать из Vault ключи
образов, нужна учётная запись в Vault. Vitastor поддерживает только аутентификацию
по клиентским сертификатам, так что все сертификаты клиентов (`cert`+`pkey`) должны
быть зарегистрированы в Vault и им должен быть дан доступ к соответствующим секретам
(поддерживается API секретов v1).

Требуемый формат секрета Vault - одно поле `key` в формате шестнадцатеричной строки.
Используется алгоритм AES-256-XTS, так что длина ключа - 64 байта, то есть строка
должна состоять из 128 шестнадцатеричных цифр.

Для подключения Vault включите следующие настройки в Vitastor.conf:
- `vault_url` - адрес Vault (например, `https://vault:8200`)
- `vault_ca` - сертификат самого Vault

После этого, если создать образ (`vitastor-cli create`) с опцией `--enc_key vault:<ID>`,
то для получения ключа клиенты Vitastor сначала обратятся к Vault для получения токена
по адресу `/v1/auth/cert/login`, а потом запросят из Vault сам секрет по адресу `/v1/secret/<ID>`.

#### Пример настройки Vault

Пошаговая инструкция для настройки тестового Vault на примере OpenBao:

1. Если ещё не настроен TLS, генерируем самоподписанный TLS сертификат для Vault:
   ```
   openssl req -days 3650 -x509 -addext basicConstraints=critical,CA:TRUE,pathlen:1 --addext subjectAltName=DNS:vault \
       -new -newkey rsa:4096 -nodes -keyout /etc/openbao/vault.key -out /etc/openbao/vault.crt
   ```
   Настраиваем его в `/etc/openbao/openbao.hcl`:
   ```
   listener "tcp" {
       address       = "0.0.0.0:8200"
       tls_cert_file = "/etc/openbao/vault.crt"
       tls_key_file  = "/etc/openbao/vault.key"
   }
   ```
   И перезапускаем OpenBao (`systemctl restart openbao`).
2. Копируем TLS сертификат Vault для Vitastor:
   ```
   cp /etc/openbao/vault.crt /etc/vitastor/vault.crt
   ```
   Переносим его на все клиентские ноды и прописываем в `/etc/vitastor/vitastor.conf`:
   ```
   {
       ...
       "vault_url": "http://vault:8200",
       "vault_ca": "/etc/vitastor/vault.crt"
   }
   ```
3. Проверяем статус Vault:
   ```
   bao status -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
4. Инициализируем Vault в тестовом режиме из 1 ноды (с 1 частью ключа):
   ```
   bao operator init -n 1 -t 1 -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
5. Разблокируем Vault:
   ```
   bao operator unseal -ca-cert /etc/openbao/vault.crt -address=https://vault:8200
   ```
6. Включаем аутентификацию по сертификатам:
   ```
   bao auth enable -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 cert
   ```
7. Включаем секреты v1:
   ```
   bao secrets enable -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 -path=secret kv-v1
   ```
8. Создаём тестовый секрет:
   ```
   bao kv put -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 secret/vitastor/testimg3 key=$(openssl rand -hex 64)
   ```
9. Генерируем подписанный сертификат для пользователя Vitastor (там, где у вас есть `client_ca.crt` и `client_ca.key`):
   ```
   openssl req -subj '/CN=testimg3' -nodes -new -keyout testimg3.key -out testimg3.csr
   openssl x509 -req -days 3650 -CA client_ca.crt -CAkey client_ca.key -CAcreateserial -in testimg3.csr -out testimg3.crt
   rm testimg3.csr
   ```
10. Создаём пользователя в Vault и даём ему доступ к секрету:
    ```
    cat >testimg3.policy <<EOF
    path "/secret/vitastor/testimg3" {
        capabilities = ["read"]
    }
    EOF

    bao policy write -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 testimg3 testimg3.policy

    bao write -ca-cert /etc/openbao/vault.crt -address=https://vault:8200 auth/cert/certs/testimg3 \
        certificate=@testimg3.crt display_name=testimg3 token_ttl=24h token_policies=testimg3
    ```
11. Тестируем доступ к секрету:
    ```
    curl --cacert /etc/vitastor/vault.crt --cert testimg3.crt --key testimg3.key \
        --json '{}' https://vault:8200/v1/auth/cert/login
    ```
    Будет выведен токен, подставляем его в следующий запрос:
    ```
    curl --cacert /etc/vitastor/vault.crt --cert testimg3.crt --key testimg3.key \
        -H 'X-Vault-Token: <ПОЛУЧЕННЫЙ ТОКЕН>' https://vault:8200/v1/secret/vitastor/testimg3
    ```
12. Создаём образ в Vitastor с заданным секретом (от имени администратора или того, кто имеет
    право создавать образы в вашем пуле):
    ```
    vitastor-cli create -s 100G --enc_key vault:vitastor/testimg3 --owner testimg3 testimg3
    ```
13. Тестируем доступ к образу от имени пользователя testimg3:
    ```
    vitastor-cli --cert testimg3.crt --pkey testimg3.key dd if=/dev/urandom oimg=testimg3 bs=1M count=100
    ```

## Списки разрешённых операций

### Права доступа к данным etcd

Ниже все названия ключей приведены без общего префикса `/vitastor`.

Разрешённые операции с ключами в Antietcd для обычных пользователей:
- Только чтение:
  - Разрешено всегда:
    - `/config/global`
    - `/config/node_placement`
    - `/config/pools`
    - `/pg/config`
    - `/osd/state/*`
    - `/pg/state/*`
    - `/index/maxid/*`
  - Для образов, которые [может читать пользователь](#пользователи-и-права-доступа):
    - `/config/inode/*`
    - `/index/image/*`
    - `/inode/stats/*`
- Чтение и запись:
  - Для пулов, в которых может создавать образы пользователь:
    - `/index/maxid/*`
  - Для образов, которыми владеет пользователь:
    - `/config/inode/*`
    - `/index/image/*`

Разрешённые операции с ключами в Antietcd для администраторов:
- Чтение:
  - `/stats`
  - `/mon/*`
  - `/pg/*`
  - `/pgstats/*`
  - `/inode/stats/*`
  - `/pool/stats/*`
- Чтение и запись:
  - `/config/*`
  - `/osd/*`
  - `/index/*`
  - `/pg/config`
  - `/pg/history/*`
  - `/stats`
  - `/history/last_clean_pgs`

Разрешённые операции с ключами в etcd для OSD:
- Чтение:
  - `/pg/config`
  - `/config/*`
- Чтение и запись:
  - `/osd/*`
  - `/pg/state/*`
  - `/pg/history/*`
  - `/pgstats/*`

Разрешённые операции с ключами в etcd для мониторов:
- Чтение:
  - `/config/*`
  - `/osd/*`
  - `/pgstats/*`
- Чтение и запись:
  - `/pg/config`
  - `/stats`
  - `/history/last_clean_pgs`
  - `/mon/*`
  - `/pg/history/*`
  - `/pg/stats/*`
  - `/pgstats/*`
  - `/inode/stats/*`
  - `/pool/stats/*`

### Права доступа к данным OSD

При включённой опции `use_perms` и шифровании OSD аутентифицирует клиентов по сертификатам
и разрешает каждому клиенту только то, что ему разрешено согласно модели прав доступа.

Клиентские операции:
- READ - разрешено для образов, доступных пользователю на чтение.
- WRITE, DELETE, SCRUB - разрешены для образов, доступных пользователю на запись.
- SYNC - операция не связана с образом и разрешена всегда.
- DESCRIBE - операция разрешена только для администраторов (используются командами
  `vitastor-cli describe` и `fix`).
- PING - операция разрешена всегда.
- SHOW_CONFIG - операция разрешена всегда, однако если в ней клиент представляется
  как OSD, то проверяется, что он использует сертификат, подписанный `osd_ca`.
- SEC_LIST (листинг) - разрешена другим OSD и администраторам с любыми параметрами,
  а обычным клиентам разрешена только для запросов, ограниченных образом, доступным
  пользователю на чтение.

Кластерные операции - разрешаются только другим OSD:
- SEC_READ
- SEC_WRITE
- SEC_WRITE_STABLE
- SEC_SYNC
- SEC_STABILIZE
- SEC_ROLLBACK
- SEC_DELETE
- SEC_READ_BMP
- SEC_LOCK

### Права доступа к API

[vitastor-cli serve](../usage/cli.ru.md#serve) также поддерживает клиентскую
аутентификацию по сертификатам. Принимаются только сертификаты, подписанные
`client_ca`. В качестве серверного сертификата используется отдельный сертификат
`api_cert` с ключом `api_pkey`.

При этом для корректной работы `vitastor-cli serve` он сам должен использовать
для доступа в Vitastor сертификат администратора (`cert`+`pkey`, подписанный `admin_ca`),
чтобы корректно обрабатывать любые запросы.

Обычным пользователям (с сертификатами, подписанными `client_ca`) при доступе к API
разрешаются только API-операции с образами, доступными им либо на чтение (для чтения),
либо на запись (для модификации):

- image/list - для образов, которые пользователь может читать.
- image/create - для пулов, в которых пользователю разрешено создавать образы, либо
  для создания снимков образов, которыми пользователь владеет.
- image/delete, image/flatten, image/modify - для образов, которыми пользователь владеет.

Все остальные операции разрешаются только администраторам.

## Производительность шифрования

У вас может возникнуть вопрос - а как быстро всё это прекрасное шифрование работает?

Ответ - сильно зависит от процессора. На современных процессорах (при наличии AVX512 с VAES)
очень быстро - скорость шифрования AES может составлять 10-20 Гбайт/с и выше. В первую очередь
подразумевается CPU клиентских машин, потому что сквозное шифрование выполняется целиком на
клиенте, а транспортное хоть также и затрагивает OSD, но у клиента поток один, а OSD на стороне
сервера много и добавить там ресурсов легче.

На более старых процессорах скорость заметно хуже, например, на Xeon E5 v4 она составляет
буквально 3 Гбайт/с.

Вы можете оценить производительность своих процессоров с помощью команды `vitastor-cli cpubench`.

Пример вывода (💪 AMD EPYC 9575F):

```
$ vitastor-cli cpubench
Vitastor transport encryption benchmark (AES-256-GCM, AES-256-XTS and xxhash3)

Warmup...

No transport encryption, data checksums enabled, e2e unencrypted image
xxhash3 1 M block... 209000 iterations in 2001 ms = 104447.78 MB/s
xxhash3 4 K block... 37000000 iterations in 2022 ms = 71479.35 MB/s

Header encryption with payload checksums, e2e unencrypted image
AES-256-GCM encrypt header + xxhash3 1 M block... 210000 iterations in 2015 ms = 104218.36 MB/s
AES-256-GCM encrypt header + xxhash3 4 K block... 26000000 iterations in 2073 ms = 48993.01 MB/s

Full transport encryption, e2e unencrypted image
AES-256-GCM encrypt header and 1 M block... 54000 iterations in 2000 ms = 27000.00 MB/s
AES-256-GCM encrypt header and 4 K block... 11700000 iterations in 2014 ms = 22692.71 MB/s

No transport encryption, no checksums, e2e encrypted image
AES-256-XTS encrypt 1 M block... 50000 iterations in 2039 ms = 24521.82 MB/s
AES-256-XTS encrypt 4 K block... 12600000 iterations in 2009 ms = 24499.13 MB/s

No transport encryption, e2e encrypted image, data checksums enabled
AES-256-XTS encrypt + xxhash3 1 M block... 40000 iterations in 2013 ms = 19870.84 MB/s
AES-256-XTS encrypt + xxhash3 4 K block... 10200000 iterations in 2011 ms = 19812.90 MB/s

Header encryption with payload checksums, e2e encrypted image
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 1 M block... 40000 iterations in 2014 ms = 19860.97 MB/s
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 4 K block... 8700000 iterations in 2011 ms = 16899.24 MB/s

Full transport encryption, e2e encrypted image
AES-256-XTS + AES-256-GCM encrypt 1 M block... 26000 iterations in 2062 ms = 12609.12 MB/s
AES-256-XTS + AES-256-GCM encrypt 4 K block... 6300000 iterations in 2006 ms = 12267.88 MB/s
```

А вот Xeon E5-2680v4:

```
$ vitastor-cli cpubench
Vitastor transport encryption benchmark (AES-256-GCM, AES-256-XTS and xxhash3)

Warmup...

No transport encryption, data checksums enabled, e2e unencrypted image
xxhash3 1 M block... 62000 iterations in 2021 ms = 30677.88 MB/s
xxhash3 4 K block... 12400000 iterations in 2006 ms = 24146.31 MB/s

Header encryption with payload checksums, e2e unencrypted image
AES-256-GCM encrypt header + xxhash3 1 M block... 62000 iterations in 2027 ms = 30587.07 MB/s
AES-256-GCM encrypt header + xxhash3 4 K block... 6800000 iterations in 2011 ms = 13208.60 MB/s

Full transport encryption, e2e unencrypted image
AES-256-GCM encrypt header and 1 M block... 7000 iterations in 2317 ms = 3021.15 MB/s
AES-256-GCM encrypt header and 4 K block... 1500000 iterations in 2102 ms = 2787.52 MB/s

No transport encryption, no checksums, e2e encrypted image
AES-256-XTS encrypt 1 M block... 7000 iterations in 2317 ms = 3021.15 MB/s
AES-256-XTS encrypt 4 K block... 1600000 iterations in 2088 ms = 2993.30 MB/s

No transport encryption, e2e encrypted image, data checksums enabled
AES-256-XTS encrypt + xxhash3 1 M block... 6000 iterations in 2188 ms = 2742.23 MB/s
AES-256-XTS encrypt + xxhash3 4 K block... 1400000 iterations in 2053 ms = 2663.78 MB/s

Header encryption with payload checksums, e2e encrypted image
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 1 M block... 6000 iterations in 2190 ms = 2739.73 MB/s
AES-256-GCM encrypt header + AES-256-XTS encrypt + xxhash3 4 K block... 1300000 iterations in 2101 ms = 2417.00 MB/s

Full transport encryption, e2e encrypted image
AES-256-XTS + AES-256-GCM encrypt 1 M block... 4000 iterations in 2666 ms = 1500.38 MB/s
AES-256-XTS + AES-256-GCM encrypt 4 K block... 800000 iterations in 2113 ms = 1478.94 MB/s
```
