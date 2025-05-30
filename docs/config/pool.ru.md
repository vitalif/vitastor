[Документация](../../README-ru.md#документация) → [Конфигурация](../config.ru.md) → Конфигурация пулов

-----

[Read in English](pool.en.md)

# Конфигурация пулов

Настройки пулов задаются в ключе etcd `/vitastor/config/pools` в JSON-формате:

```
{
  "<Численный ID>": {
    "name": "<имя>",
    ...остальные параметры...
  }
}
```

На настройку пулов также влияют:

- [Дерево размещения OSD](#дерево-размещения)
- [Настройки отдельных OSD](#настройки-osd)

Параметры:

- [name](#name)
- [scheme](#scheme)
- [pg_size](#pg_size)
- [parity_chunks](#parity_chunks)
- [pg_minsize](#pg_minsize)
- [pg_count](#pg_count)
- [failure_domain](#failure_domain)
- [level_placement](#level_placement)
- [raw_placement](#raw_placement)
- [local_reads](#local_reads)
- [max_osd_combinations](#max_osd_combinations)
- [block_size](#block_size)
- [bitmap_granularity](#bitmap_granularity)
- [immediate_commit](#immediate_commit)
- [pg_stripe_size](#pg_stripe_size)
- [root_node](#root_node)
- [osd_tags](#osd_tags)
- [primary_affinity_tags](#primary_affinity_tags)
- [scrub_interval](#scrub_interval)
- [used_for_app](#used_for_app)

Примеры:

- [Реплицированный пул](#реплицированный-пул)
- [Пул с кодами коррекции ошибок 2+1](#пул-с-кодами-коррекции-ошибок)

# Дерево размещения

Дерево размещения OSD задаётся в отдельном ключе etcd `/vitastor/config/node_placement`
в следующем JSON-формате:

```
{
  "<имя узла или номер OSD>": {
    "level": "<уровень>",
    "parent": "<имя родительского узла, если есть>"
  },
  ...
}
```

Здесь, если название узла - число, считается, что это OSD. Уровень OSD
всегда равен "osd" и не может быть переопределён. Для OSD вы можете только
переопределить родительский узел. По умолчанию родителем OSD считается его хост.

Нечисловые имена узлов относятся к другим узлам дерева OSD, таким, как хосты (серверы),
стойки, датацентры и так далее.

Хосты всех OSD автоматически создаются в дереве с уровнем "host" и именем, равным имени хоста,
сообщаемым соответствующим OSD. Вы можете ссылаться на эти хосты, не заводя их
в дереве вручную.

Уровень может быть "host", "osd" или относиться к другому уровню размещения из
[placement_levels](monitor.ru.md#placement_levels).

Родительский узел нужен только для промежуточных узлов дерева.

# Настройки OSD

Настройки отдельных OSD задаются в ключах etcd `/vitastor/config/osd/<number>`
в JSON-формате `{"<key>":<value>}`.

На данный момент поддерживаются следующие настройки:

- [reweight](#reweight)
- [tags](#tags)
- [noout](#noout)

## reweight

- Тип: число, от 0 до 1
- По умолчанию: 1

Каждый OSD получает число PG, пропорциональное его размеру. Reweight - это
множитель для размера, используемый в процессе распределения PG.

Это значит, что OSD, сконфигурированный с reweight меньше 1 будет получать
меньше PG, чем обычно. OSD с reweight, равным 0, не будет участвовать в
хранении данных вообще. Вы можете установить reweight в 0, чтобы убрать
все данные с OSD.

## tags

- Тип: строка или массив строк

Задаёт тег или набор тегов для данного OSD. Теги можно использовать, чтобы
делить OSD на множества и потом размещать пул только на части OSD, а не на
всех. Можно, например, пометить SSD OSD тегом "ssd", а HDD тегом "hdd", в
этом смысле теги работают аналогично классам устройств.

## noout

- Тип: булево (да/нет)
- Значение по умолчанию: false

Если установлено в true, то [osd_out_time](monitor.ru.md#osd_out_time) для этого
OSD игнорируется и OSD не удаляется из распределения данных монитором.

# Параметры

## name

- Тип: строка
- Обязательный

Название пула.

## scheme

- Тип: строка
- Возможные значения: "replicated", "xor", "ec" или "jerasure"
- Обязательный

Схема избыточности, используемая в данном пуле. "jerasure" - синоним для "ec",
в обеих схемах используются коды Рида-Соломона-Вандермонда, реализованные на
основе библиотек ISA-L или jerasure. Быстрая реализация на основе ISA-L
используется автоматически, когда доступна, в противном случае используется
более медленная jerasure-версия.

## pg_size

- Тип: целое число
- Обязательный

Размер PG данного пула, т.е. число реплик для реплицированных пулов или
число дисков данных плюс дисков чётности для пулов EC/XOR.

## parity_chunks

- Тип: целое число

Число дисков чётности для EC/XOR пулов. Иными словами, число дисков, при
одновременной потере которых данные будут потеряны.

Игнорируется для реплицированных пулов, обязательно для EC/XOR.

## pg_minsize

- Тип: целое число
- Обязательный

Число доступных дисков для PG данного пула, при котором PG остаются активны.
Если становится невозможно размещать новые данные в PG как минимум на pg_minsize
OSD, PG деактивируется на чтение и запись. Иными словами, всегда известно,
что новые блоки данных всегда записываются как минимум на pg_minsize дисков.

Для примера, разница между pg_minsize 2 и 1 в реплицированном пуле с 3 копиями
данных (pg_size=3), проявляется следующим образом:
- Если 2 сервера отключаются при pg_minsize=2, пул становится неактивным и
  остаётся неактивным в течение [osd_out_time](monitor.ru.md#osd_out_time)
  (10 минут), после чего монитор назначает другие OSD/серверы на замену, пул
  поднимается и начинает восстанавливать недостающие копии данных. Соответственно,
  если OSD на замену нет - то есть, если у вас всего 3 сервера с OSD и 2 из них
  недоступны - пул так и остаётся недоступным до тех пор, пока вы не вернёте
  или не добавите хотя бы 1 сервер (или не переключите failure_domain на "osd").
- Если 2 сервера отключаются при pg_minsize=1, ввод-вывод лишь приостанавливается
  на короткое время, до тех пор, пока монитор не поймёт, что OSD отключены
  (что занимает 5-10 секунд при стандартном [etcd_report_interval](osd.ru.md#etcd_report_interval)).
  После этого ввод-вывод восстанавливается, но новые данные временно пишутся
  всего в 1 копии. Когда же проходит osd_out_time, монитор точно так же назначает
  другие OSD на замену выбывшим и пул начинает восстанавливать копии данных.

То есть, pg_minsize регулирует число отказов, которые пул может пережить без
временной остановки обслуживания на [osd_out_time](monitor.ru.md#osd_out_time),
но ценой немного пониженных гарантий надёжности.

FIXME: Поведение pg_minsize может быть изменено в будущем с полной деактивации
PG на перевод их в режим только для чтения.

## pg_count

- Тип: целое число
- Обязательный

Число PG для данного пула. Число должно быть достаточно большим, чтобы монитор
мог равномерно распределить по ним данные.

Обычно это означает примерно 10-100 PG на 1 OSD, т.е. pg_count можно устанавливать
равным (общему числу OSD * 10 / pg_size). Значение можно округлить до ближайшей
степени 2, чтобы потом было легче уменьшать или увеличивать число PG, умножая
или деля его на 2.

PG в Vitastor эферемерны, то есть вы можете менять их число в любой момент,
просто перезаписывая конфигурацию пулов в etcd. Однако объём перемещения данных
при этом будет минимален, если новое число PG кратно старому (или наоборот).

## failure_domain

- Тип: строка
- По умолчанию: host

Домен отказа для пула. Может быть равен "host" или "osd" или любому другому
уровню дерева OSD, задаваемому в настройке [placement_levels](monitor.ru.md#placement_levels).

Смысл домена отказа в том, что 2 копии, или 2 части одного блока данных в случае
кодов коррекции ошибок, никогда не помещаются на OSD, принадлежащие одному домену отказа.
Иными словами, домен отказа - это то, от отказа чего вы защищаете себя избыточным
хранением.

## level_placement

- Тип: строка

Правила дополнительных доменов отказа, применяемые вместе с failure_domain.
Должны задаваться в следующем виде:

`<уровень>=<последовательность символов>, <уровень2>=<последовательность2>, ...`

Каждая `<последовательность>` должна состоять ровно из [pg_size](#pg_size) символов.
Каждый символ соответствует одному OSD (размещению одной части PG) этого пула.
Одинаковые символы означают, что соответствующие части размещаются в один и тот же
узел дерева OSD на заданном `<уровне>`. Разные символы означают, что части
размещаются в разные узлы.

Например, если вы хотите сделать пул EC 4+2 и хотите поместить каждые 2 части
данных в свой датацентр, и также вы хотите, чтобы каждая часть размещалась на
другом хосте, то вы должны задать `level_placement` равным `dc=112233 host=123456`.

Либо вы просто можете задать `level_placement` равным `dc=112233` и оставить
`failure_domain` пустым, т.к. `host` это его значение по умолчанию и оно также
применится автоматически.

Без этого правила может получиться так, что в одном из датацентров окажется
3 части данных одной PG и данные окажутся недоступными при временном отключении
этого датацентра.

Естественно, перед установкой правила вам нужно сгруппировать ваши хосты в
датацентры, установив [placement_levels](monitor.ru.md#placement_levels) во что-то
типа `{"dc":90,"host":100,"osd":110}` и добавив датацентры в [node_placement](#дерево-размещения),
примерно так: `{"dc1":{"level":"dc"},"host1":{"parent":"dc1"},...}`.

## raw_placement

- Тип: строка

Низкоуровневые правила генерации PG в форме DSL (доменно-специфичного языка).
Используйте, только если действительно знаете, зачем вам это надо :)

Спецификация DSL:

```
dsl := item | item ("\n" | ",") items
item := "any" | rules
rules := rule | rule rules
rule := level operator arg
level := /\w+/
operator := "!=" | "=" | ">" | "?="
arg := value | "(" values ")"
values := value | value "," values
value := item_ref | constant_id
item_ref := /\d+/
constant_id := /"([^"]+)"/
```

Оператор "?=" означает "предпочитаемый". Т.е. `dc ?= "meow"` означает "предпочитать
датацентр meow для этой части данных, но разместить её в другом датацентре, если
meow недоступен".

Примеры:

- Простые 3 реплики с failure_domain=host: `any, host!=1, host!=(1,2)`
- EC 4+2 в 3 датацентрах: `any, dc=1 host!=1, dc!=1, dc=3 host!=3, dc!=(1,3), dc=5 host!=5`
- 1 копия в фиксированном ДЦ + 2 в других ДЦ: `dc?=meow, dc!=1, dc!=(1,2)`

## local_reads

- Тип: строка
- Возможные значения: "primary", "nearest" или "random"
- По умолчанию: primary

По умолчанию Vitastor обслуживает все запросы чтения и записи с первичного OSD каждой PG.
Однако, в чистых PG (active или active+left_on_dead) реплицированных пулов также есть
возможность обслуживать запросы чтения с вторичных OSD, что может быть полезно, если
у вас сильно отличается время сетевого обращения от клиента к разным OSD - например,
если у вас несколько дата-центров.

Если данный параметр установлен в значение "nearest", клиенты будут стараться читать с
ближайших по [Дереву размещения](#дерево-размещения) OSD, то есть, с OSD с того же хоста
или датацентра. Расстояние до разных OSD будет рассчитываться с помощью имени хоста клиента,
определяемого автоматически или заданного вручную параметром [hostname](client.ru.md#hostname).

Если данный параметр установлен в значение "random", клиенты будут стараться распределять
запросы чтения по всем доступным вторичным OSD. Этот режим в основном полезен для тестов,
но, скорее всего, редко нужен в реальных инсталляциях.

Для работы локальных чтений требуются [блокировки PG](osd.ru.md#enable_pg_locks). Включать
их явно не нужно - они включаются автоматически для пулов с включёнными локальными чтениями.

## max_osd_combinations

- Тип: целое число
- По умолчанию: 10000

Алгоритм распределения данных Vitastor основан на решателе задачи линейного
программирования. При этом для снижения сложности задачи возможные комбинации OSD
генерируются случайно и ограничиваются количеством, равным значению этого параметра.

Обычно данный параметр не требует изменений.

## block_size

- Тип: целое число
- По умолчанию: 131072

Размер блока для данного пула. Если не задан, используется значение из
/vitastor/config/global. Если в вашем кластере есть OSD с разными размерами
блока, пул будет использовать только OSD с размером блока, равным размеру блока
пула. Если вы хотите сильнее ограничить набор используемых для пула OSD -
используйте [osd_tags](#osd_tags).

О самом параметре читайте в разделе [Дисковые параметры уровня кластера](layout-cluster.ru.md#block_size).

## bitmap_granularity

- Тип: целое число
- По умолчанию: 4096

Размер "сектора" виртуальных дисков в данном пуле. Если не задан, используется
значение из /vitastor/config/global. Аналогично block_size, каждый пул будет
использовать только OSD с совпадающей с пулом настройкой bitmap_granularity.

О самом параметре читайте в разделе [Дисковые параметры уровня кластера](layout-cluster.ru.md#bitmap_granularity).

## immediate_commit

- Тип: строка
- Возможные значения: "all", "small" или "none"
- По умолчанию: none

Настройка мгновенного коммита для данного пула. Если не задана, используется
значение из /vitastor/config/global. Аналогично block_size, каждый пул будет
использовать только OSD с *совместимыми* настройками immediate_commit.
"Совместимыми" означает, что пул с отключенным мгновенным коммитом будет
использовать OSD с включённым мгновенным коммитом, но не наоборот. То есть,
пул со значением "none" будет использовать все OSD, пул со "small" будет
использовать OSD с "all" или "small", а пул с "all" будет использовать только
OSD с "all".

О самом параметре читайте в разделе [Дисковые параметры уровня кластера](layout-cluster.ru.md#immediate_commit).

## pg_stripe_size

- Тип: целое число
- По умолчанию: 0

Данный параметр задаёт размер полосы "нарезки" образов на PG. Размер полосы не может
быть меньше, чем [block_size](#block_size), умноженный на
(pg_size - parity_chunks) для EC-пулов или 1 для реплицированных пулов. То же
значение используется по умолчанию.

Это означает, что по умолчанию первые `pg_stripe_size = (block_size * (pg_size-parity_chunks))` байт
образа помещаются в одну PG, следующие `pg_stripe_size` байт помещаются в другую
и т.п.

Данный параметр обычно тоже не требует изменений.

## root_node

- Тип: строка

Корневой узел дерева OSD для ограничения OSD, выбираемых для пула. Задаваемый
узел должен быть предварительно задан в /vitastor/config/node_placement.

## osd_tags

- Тип: строка или массив строк

Теги OSD для ограничения OSD, выбираемых для пула. Если задаётся несколько тегов
массивом, то выбираются только OSD, у которых есть все эти теги.

## primary_affinity_tags

- Тип: строка или массив строк

Теги OSD, по которым должны выбираться OSD, предпочитаемые в качестве первичных
для PG этого пула. Имейте в виду, что для EC-пулов Vitastor также всегда
предпочитает помещать первичный OSD на один из OSD с данными, а не с чётностью.

## scrub_interval

- Тип: временной интервал (число + единица измерения s/m/h/d/M/y)

Интервал скраба, то есть, автоматической фоновой проверки данных для данного пула.
Переопределяет [глобальную настройку scrub_interval](osd.ru.md#scrub_interval).

## used_for_app

- Тип: строка

Если непусто, пул помечается как используемый для отдельного приложения, например,
для VitastorFS или S3, которое распределяет ID образов в пуле само и не использует
метаданные образов/инодов в etcd.

Когда пул помечается используемым для такого приложения, создание обычных блочных
образов в нём запрещается (vitastor-cli отказывается создавать образы без --force),
чтобы защитить пользователя от коллизий ID блочных образов и томов ФС/S3, и,
таким образом, от потери данных.

Также для таких пулов отключается передача статистики в etcd по отдельным инодам,
так как использование для внешнего приложения подразумевает, что пул может содержать
очень много томов и их статистика может занять слишком много места в etcd.

Установка used_for_app в значение `fs:<name>` сообщает о том, что пул используется
для VitastorFS с базой метаданных VitastorKV, хранимой в блочном образе с именем
`<name>`.

[vitastor-nfs](../usage/nfs.ru.md), в свою очередь, при запуске отказывается
использовать для ФС пулы, не помеченные, как используемые для неё. Это также
означает, что один пул может использоваться только для одной VitastorFS.

Если же вы планируете использовать пул для данных S3, установите его used_for_app
в значение `s3:<name>`, где `<name>` - любое название по вашему усмотрению
(например, `s3:standard`) - конкретное содержимое `<name>` пока никак не проверяется
компонентами Vitastor S3.

Смотрите также [allow_net_split](osd.ru.md#allow_net_split) и
[документацию по состояниям PG](../usage/admin.ru.md#состояния-pg).

Все остальные значения used_for_app, кроме начинающихся на `fs:` или `s3:`, не
означают ничего особенного для основных компонентов Vitastor. Поэтому сейчас вы
можете использовать их свободно любым желаемым способом.

# Примеры

## Реплицированный пул

```
{
  "1": {
    "name":"testpool",
    "scheme":"replicated",
    "pg_size":2,
    "pg_minsize":1,
    "pg_count":256,
    "failure_domain":"host"
  }
}
```

## Пул с кодами коррекции ошибок

```
{
  "2": {
    "name":"ecpool",
    "scheme":"ec",
    "pg_size":3,
    "parity_chunks":1,
    "pg_minsize":2,
    "pg_count":256,
    "failure_domain":"host"
  }
}
```
