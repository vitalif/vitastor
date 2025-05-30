[Документация](../../README-ru.md#документация) → Введение → Архитектура

-----

[Read in English](architecture.en.md)

# Архитектура

Краткое описание архитектуры Vitastor.

- [Серверные компоненты](#серверные-компоненты)
- [Базовые понятия](#базовые-понятия)
- [Клиентские компоненты](#клиентские-компоненты)
- [Дополнительные утилиты](#дополнительные-утилиты)
- [Общий процесс записи и чтения](#общий-процесс-записи-и-чтения)
  - [Особенности обработки запросов](#особенности-обработки-запросов)
- [Схожесть с Ceph](#схожесть-с-ceph)
- [Отличия от Ceph](#отличия-от-ceph)
- [Принципы разработки](#принципы-разработки)

## Серверные компоненты

- **OSD** (Object Storage Daemon) — процесс, который непосредственно работает с диском, пишет и читает данные.
  Один OSD управляет одним диском (или разделом). OSD общаются с etcd и друг с другом — от etcd они
  получают состояние кластера, а друг другу передают запросы записи и чтения вторичных копий данных.
- **etcd** — кластерная key/value база данных, используется для хранения настроек и верхнеуровневого
  состояния кластера, а также предотвращения разделения сознания (splitbrain). Блоки данных в etcd не
  хранятся, в обработке клиентских запросов чтения и записи etcd не участвует.
- **Монитор** — отдельный демон на node.js, рассчитывающий необходимые изменения в конфигурацию
  кластера, сохраняющий эту информацию в etcd и таким образом командующий OSD применить эти изменения.
  Также агрегирует статистику. Контактирует только с etcd, OSD с монитором не общаются.

## Базовые понятия

- **Пул (Pool)** — контейнер для данных, имеющих одну и ту же схему избыточности и правила распределения по OSD.
- **PG (Placement Group)** — "шард", единица деления пулов в кластере, которой назначается свой набор
  OSD для хранения данных (копий или частей объектов).
- **Домен отказа (Failure Domain)** — группа OSD, от одновременного падения которых должен защищать
  Vitastor. По умолчанию домен отказа — "host" (сервер), но вы можете установить для пула как больший
  домен отказа (например, стойку серверов), так и меньший (например, отдельный диск).
- **Дерево распределения** (Placement Tree, в Ceph CRUSH Tree) — иерархическая группировка OSD
  в узлы, которые далее можно использовать как домены отказа.

## Клиентские компоненты

- **Клиентская библиотека** — инкапсулирует логику на стороне клиента. Соединяется с etcd и со всеми OSD,
  от etcd получает состояние кластера, команды чтения и записи отправляет на все OSD напрямую.
  В силу архитектуры все отдельные блоки данных (по умолчанию по 128 КБ) располагается на разных
  OSD, но клиент устроен так, что всегда точно знает, к какому OSD обращаться, и подключается
  к нему напрямую.

На базе клиентской библиотеки реализованы все остальные клиенты:

- **[vitastor-cli](../usage/cli.ru.md)** — утилита командной строки для управления кластером.
  Позволяет просматривать общее состояние кластера, управлять пулами и образами — то есть
  создавать, менять и удалять виртуальные диски, их снимки и клоны.
- **[Драйвер QEMU](../usage/qemu.ru.md)** — подключаемый модуль QEMU, позволяющий QEMU/KVM
  виртуальным машинам работать с виртуальными дисками Vitastor напрямую из пространства пользователя
  с помощью клиентской библиотеки, без необходимости подключения дисков в виде блочных устройств
  Linux. Если, однако, вы хотите подключать диски в виде блочных устройств, то вы тоже можете
  сделать это с помощью того же самого драйвера и [VDUSE](../usage/qemu.ru.md#vduse).
- **[vitastor-nbd](../usage/nbd.ru.md)** — утилита, позволяющая монтировать образы Vitastor
  в виде блочных устройств с помощью NBD (Network Block Device), на самом деле скорее работающего
  как "BUSE" (Block Device In Userspace). Модуля ядра Linux для выполнения той же задачи в
  Vitastor нет (по крайней мере, пока). NBD — более старый и нерекомендуемый способ подключения
  дисков — вам следует использовать VDUSE всегда, когда это возможно.
- **[CSI драйвер](../installation/kubernetes.ru.md)** — драйвер для подключения Vitastor-образов
  и поддиректорий VitastorFS в виде персистентных томов (PV) Kubernetes. Блочный CSI работает через
  VDUSE (когда это возможно) или через NBD — образы отражаются в виде блочных устройств и монтируются
  в контейнеры. Файловый CSI использует **[vitastor-nfs](../usage/nfs.ru.md)**.
- **Драйвера Proxmox, OpenStack и т.п.** — подключаемые модули для соответствующих систем,
  позволяющие использовать Vitastor как хранилище в оных.
- **[vitastor-nfs](../usage/nfs.ru.md)** — NFS 3.0 сервер, предоставляющий два варианта файловой системы:
  первая — упрощённая для файлового доступа к блочным образам (для не-QEMU гипервизоров, поддерживающих NFS),
  вторая — VitastorFS, полноценная кластерная POSIX ФС. Оба варианта поддерживают параллельный
  доступ с нескольких vitastor-nfs серверов. На самом деле можно вообще не выделять
  отдельные NFS-серверы, а вместо этого использовать команду vitastor-nfs mount, запускающую
  NFS-сервер прямо на клиентской машине и монтирующую ФС локально.
- **[Драйвер fio](../usage/fio.ru.md)** — подключаемый модуль для утилиты тестирования
  производительности дисков fio, позволяющий тестировать Vitastor-кластеры.
- **vitastor-kv** — клиент для key-value базы данных, работающей поверх разделяемого блочного
  образа (обычного блочного образа vitastor). Метаданные VitastorFS хранятся именно в vitastor-kv.

## Дополнительные утилиты

- **vitastor-disk** — утилита для разметки дисков под Vitastor OSD. С её помощью можно
  создавать, удалять, менять размеры или перемещать разделы OSD.

## Общий процесс записи и чтения

- В Vitastor хранятся виртуальные диски, также называемые "образы" или "иноды".
- Каждый образ хранится в определённом пуле. Пул определяет параметры хранения образов в нём,
  такие, как схему избыточности (репликация или EC — коды коррекции ошибок), домен отказа
  и ограничения по выбору OSD для размещения данных образов. См. [Конфигурация пулов](../config/pool.ru.md).
- Каждый образ разбивается на объекты фиксированного размера, равного [block_size](../config/layout-cluster.ru.md#block_size)
  (по умолчанию 128 КБ), умноженному на число частей данных для EC или на 1 для реплик.
  То есть, например, если в пуле используется схема кодирования 4+2 (4 части данных + 2 чётности),
  то при стандартном block_size образы разбиваются на части по 512 КБ.
- Клиентские запросы чтения/записи разделяются на части, соответствующие этим объектам.
- Для каждого объекта определяется номер PG, которой он принадлежит, путём простого взятия
  остатка от деления ID образа и смещения на число PG в пуле, которому принадлежит образ.
- Клиент читает информацию о первичном OSD всех PG из etcd. Первичный OSD каждой PG назначается
  монитором в процессе работы кластера, вместе с текущим целевым набором OSD этой PG.
- Клиент соединяется (если ещё не соединён) с первичными OSD всех PG, объекты которых участвуют
  в запросе и одновременно отправляет им запросы чтения/записи частей.
- Если какие-то из первичных OSD недоступны, клиент повторяет попытки соединения бесконечно до
  тех пор, пока они не станут доступны или пока PG не будут назначены другие первичные OSD.
- Клиент также повторяет запросы, если первичные OSD отвечают кодом ошибки EPIPE, означающим,
  что запрос не может быть обработан в данный момент. Это происходит, если PG либо не активна
  вообще, либо не активна на данном OSD в данный момент (например, если в этот момент меняется
  её первичный OSD) или если в процессе обработки запроса сам первичный OSD теряет подключение
  к репликам.
- Первичный OSD определяет, на каких OSD находятся части объекта. По умолчанию все объекты
  считаются находящимися на текущем целевом наборе OSD соответствующей PG, но какие-то могут
  находиться на других OSD, если эти объекты деградированы или перемещены, или идёт процесс
  ребаланса. Запросы для проверки по сети не отправляются, информация о местоположении всех
  объектов рассчитывается первичным OSD при активации PG и хранится в памяти.
- Когда это возможно, первичный OSD обрабатывает запрос локально. Например, так происходит
  при чтениях объектов из пулов с репликацией или при чтении из EC пула, затрагивающего
  только часть, хранимую на диске самого первичного OSD.
- Когда запрос требует записи или чтения с вторичных OSD, первичный OSD использует заранее
  установленные соединения с ними для выполнения этих запросов. Это происходит параллельно
  локальным операциям чтения/записи с диска самого OSD. Так как соединения к вторичным OSD PG
  устанавливаются при её запуске, то они уже гарантированно установлены, когда PG активна,
  и если любое из этих соединений отключается, PG перезапускается, а все текущие запросы чтения
  и записи в неё завершаются с ошибкой EPIPE, после чего повторяются клиентами.
- После завершения всех вторичных операций чтения/записи первичный OSD отправляет ответ клиенту.
- Если в реплицированном пуле включены [локализованные чтения](../config/pool.ru.md#local_reads),
  а PG находится в чистом активном состоянии (active или active+left_on_dead), клиент может
  послать запрос к одному из вторичных OSD вместо первичного. Вторичный OSD проверяет
  [блокировку PG](../config/osd.ru.md#enable_pg_locks) и обрабатывает запрос локально, не
  обращаясь к первичному. Блокировка PG здесь нужна, чтобы вторичный OSD мог точно знать,
  что PG находится в чистом состоянии и не переключается на другой первичный OSD.

### Особенности обработки запросов

- Если в пуле используются коды коррекции ошибок и при этом часть OSD недоступна, первичный
  OSD при чтении восстанавливает данные из оставшихся частей.
- Каждый объект имеет номер версии. При записи объекта первичный OSD сначала получает номер
  версии объекта. Так как первичный OSD обычно сам хранит копию или часть объекта, номер
  версии обычно читается из памяти самого OSD. Однако, если ни одна часть обновляемого объекта
  не находится на первичном OSD, для получения номера версии он обращается к одному из вторичных
  OSD, на которых копия объекта есть. Обращения к диску при этом всё равно не происходит,
  так как метаданные объектов, включая номер версии, все OSD хранят в памяти.
- Если в пуле используются коды коррекции ошибок, перед частичной записью объекта для вычисления
  чётности зачастую требуется чтение частей объекта с вторичных OSD или с локального диска
  самого первичного OSD. Это называется процессом "чтение-модификация-запись" (read-modify-write).
- Если в пуле используются коды коррекции ошибок, для закрытия Write Hole применяется
  двухфазный алгоритм записи: сначала на все вторичные OSD записывается новая версия частей
  объекта, но при этом старая версия не удаляется, а потом, после получения подтверждения
  успешной записи от всех вторичных OSD, новая версия фиксируется и разрешается удаление старой.
- Если в пуле не включён режим immediate_commit, то запросы записи, отправляемые клиентами,
  не считаются зафиксированными на физических накопителях сразу. Для фиксации данных клиенты
  должны отдельно отправлять запросы SYNC (отдельный от чтения и записи вид запроса),
  а пока такой запрос не отправлен, считается, что записанные данные могут исчезнуть,
  если соответствующий OSD упадёт. Поэтому, когда режим immediate_commit отключён, все
  запросы записи клиенты копируют в памяти и при потере соединения и повторном соединении
  с OSD повторяют из памяти. Скопированные в память данные удаляются при успешном SYNC,
  а чтобы хранение этих данных не приводило к чрезмерному потреблению памяти, клиенты
  автоматически выполняют SYNC каждые [client_dirty_limit](../config/network.ru.md#client_dirty_limit)
  записанных байт.

## Схожесть с Ceph

- В Vitastor тоже есть пулы (pools), PG, OSD, мониторы, домены отказы, дерево распределения (аналог crush-дерева).
- Vitastor тоже равномерно распределяет данные каждого образа по всем PG пула.
- Vitastor тоже транзакционный, то есть, каждая запись в кластер атомарна.
- У Vitastor OSD тоже есть журнал и метаданные и их тоже можно размещать на отдельном диске.
- Как и в Ceph, клиентская библиотека пытается дождаться восстановления работы
  при любом полном или частичном отказе кластера, то есть, вы можете перезагрузить
  хоть весь кластер разом, и клиенты не отключатся, только на время зависнут.

## Отличия от Ceph

- Vitastor в первую очередь сфокусирован на использовании с SSD: либо в кластерах на основе
  только SSD, либо гибридных (HDD с журналами на SSD).
- Базовый слой Vitastor — простое блочное хранилище с блоками фиксированного размера, а не
  объектное со сложной семантикой, как в Ceph (RADOS).
- PG в Vitastor эфемерны. Это означает, что они не хранятся на дисках и существуют только в
  памяти работающих OSD.
- Vitastor OSD однопоточные и всегда такими останутся. Если вам нужно выделить больше 1 ядра
  на 1 диск — создайте несколько OSD на разделах этого диска. Нужно это в основном для NVMe,
  так как Vitastor не потребляет много ресурсов CPU.
- Метаданные всегда размещаются в памяти, благодаря чему никогда не тратится лишнее время
  на чтение метаданных с диска. Объём метаданных линейно зависит от ёмкости диска и размера
  блока хранилища (block_size, по умолчанию 128 КБ). С 128 КБ блоком потребление памяти
  составляет примерно 512 МБ на 1 ТБ данных. Журналы по умолчанию тоже хранятся в памяти,
  но в SSD-кластерах нужный размер журнала составляет всего 32 МБ, а в гибридных (SSD+HDD)
  кластерах, в которых есть смысл делать журналы больше, можно отключить [inmemory_journal](../config/osd.ru.md#inmemory_journal).
- В Vitastor нет внутреннего copy-on-write. Я считаю, что реализация CoW-хранилища гораздо сложнее,
  поэтому сложнее добиться устойчиво хороших результатов. Возможно, в один прекрасный день
  я придумаю красивый алгоритм для CoW-хранилища, но пока нет — внутреннего CoW в Vitastor не будет.
  Всё это не относится к "внешнему" CoW (снапшотам и клонам).
- В Vitastor есть режим "ленивых fsync", в котором OSD группирует запросы записи перед сбросом их
  на диск, что позволяет получить лучшую производительность с дешёвыми настольными SSD без конденсаторов
  ("Advanced Power Loss Protection" / "Capacitor-Based Power Loss Protection").
  Тем не менее, такой режим всё равно медленнее использования нормальных серверных SSD и мгновенного
  fsync, так как приводит к дополнительным операциям передачи данных по сети, поэтому рекомендуется
  всё-таки использовать хорошие серверные диски, тем более, стоят они почти так же, как десктопные.
- Процессы восстановления оперируют отдельными объектами, а не целыми PG.
- "Мониторы" не хранят данные. Конфигурация и состояние кластера хранятся в etcd в простых человекочитаемых
  JSON-структурах. Мониторы Vitastor только следят за состоянием кластера и управляют перемещением данных.
  В этом смысле монитор Vitastor не является критичным компонентом системы и больше похож на Ceph-овский
  менеджер (MGR). Монитор Vitastor написан на node.js.
- Распределение PG не основано на консистентных хешах. Вместо этого все маппинги PG хранятся прямо в etcd
  (ибо нет никакой проблемы сохранить несколько сотен-тысяч записей в памяти, а не считать каждый раз хеши).
  Перераспределение PG по OSD выполняется через математическую оптимизацию,
  а конкретно, сведение задачи к ЛП (задаче линейного программирования) и решение оной с помощью утилиты
  lp_solve. Такой подход позволяет обычно выравнивать распределение места почти идеально — равномерность
  обычно составляет 96-99%, в отличие от Ceph, где на голом CRUSH-е без балансировщика обычно выходит 80-90%.
  Также это позволяет минимизировать объём перемещения данных и случайность связей между OSD, а также менять
  распределение вручную, не боясь сломать логику перебалансировки. В таком подходе есть и потенциальный
  недостаток — есть предположение, что в очень большом кластере он может сломаться — однако вплоть до
  нескольких сотен OSD подход точно работает нормально. Ну и, собственно, при необходимости легко
  реализовать и консистентные хеши.
- Отдельный слой, подобный слою "CRUSH-правил", отсутствует. Вы настраиваете схемы отказоустойчивости,
  домены отказа и правила выбора OSD напрямую в конфигурации пулов.

## Принципы разработки

- Я люблю простые решения. Поэтому Vitastor простой и быстрый и всегда будет таковым.
- Я не против иногда изобрести велосипед, например, собственный простенький HTTP-клиент
  для работы с etcd, вместо того, чтобы взять тяжёлую готовую библиотеку, ибо в данном
  случае я точно уверен в том, что знаю, что делает код.
- Общепринятые практики написания C++ кода с RAII, наследованием, умными указателями
  и так далее меня также не волнуют, поэтому если вы хотели увидеть здесь красивый
  идиоматичный C++ код, вы, вероятно, пришли не по адресу.
- Из всех интерпретаторов скриптоты больше всех я люблю node.js, это самый быстрый в мире
  интерпретатор, у него есть встроенная событийная машина, развитая инфраструктура и
  приятный нейтральный C-подобный язык программирования. Поэтому Монитор реализован на node.js.

## Известные проблемы

- Удаление образов в деградированном кластере может в данный момент приводить к повторному
  "появлению" удалённых объектов после поднятия отключённых OSD, причём в случае EC-пулов,
  объекты могут появиться в виде "неполных". Если вы столкнётесь с такой ситуацией, просто
  повторите запрос удаления. Данная проблема будет исправлена в будущем вместе с обновлением
  дискового формата хранения метаданных.
