[Документация](../../README-ru.md#документация) → Введение → Быстрый старт

-----

[Read in English](quickstart.en.md)

# Быстрый старт

- [Подготовка](#подготовка)
- [Рекомендуемые диски](#рекомендуемые-диски)
- [Настройте мониторы](#настройте-мониторы)
- [Настройте OSD](#настройте-osd)
- [Создайте пул](#создайте-пул)
- [Проверьте состояние кластера](#проверьте-состояние-кластера)
- [Создайте образ](#создайте-образ)
- [Установите плагины](#установите-плагины)
- [Создайте VitastorFS](#создайте-vitastorfs)

## Подготовка

- Возьмите серверы с SSD (SATA или NVMe), желательно с конденсаторами (серверные SSD). Можно
  использовать и десктопные SSD, включив режим отложенного fsync, но производительность будет хуже.
  О конденсаторах читайте [здесь](../config/layout-cluster.ru.md#immediate_commit).
- Если хотите использовать HDD, берите современные модели с Media или SSD кэшем - HGST Ultrastar,
  Toshiba MG, Seagate EXOS или что-то похожее. Если такого кэша у ваших дисков нет,
  обязательно возьмите SSD под метаданные и журнал (маленькие, буквально 2 ГБ на 1 ТБ HDD-места).
- Возьмите быструю сеть, минимум 10 гбит/с. Идеал - что-то вроде Mellanox ConnectX-4 с RoCEv2.
- Для лучшей производительности отключите энергосбережение CPU: `cpupower idle-set -D 0 && cpupower frequency-set -g performance`.
- Либо [установите пакеты Vitastor](../installation/packages.ru.md), либо [установите Vitastor в Docker](../installation/docker.ru.md).

## Рекомендуемые диски

- SATA SSD: Micron 5100/5200/5300/5400, Samsung PM863/PM883/PM893, Intel D3-S4510/4520/4610/4620, Kingston DC500M
- NVMe: Micron 9100/9200/9300/9400, Micron 7300/7450, Samsung PM983/PM9A3, Samsung PM1723/1735/1743,
  Intel DC-P3700/P4500/P4600, Intel D5-P4320/P5530, Intel D7-P5500/P5600, Intel Optane, Kingston DC1000B/DC1500M
- HDD: HGST Ultrastar, Toshiba MG, Seagate EXOS

## Настройте мониторы

На хостах, выделенных под мониторы:
- Пропишите одинаковые etcd_address в `/etc/vitastor/vitastor.conf`. Например:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"]
  }
  ```
- Инициализируйте сервисы etcd, запустив `/usr/lib/vitastor/mon/make-etcd`.\
  Либо, если вы установили Vitastor в Docker, запустите `systemctl start vitastor-host; docker exec vitastor make-etcd`.
- Запустите etcd и мониторы: `systemctl enable --now vitastor-etcd vitastor-mon`

## Настройте OSD

- Пропишите etcd_address и [osd_network](../config/network.ru.md#osd_network) в `/etc/vitastor/vitastor.conf`. Например:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"],
    "osd_network": "10.200.1.0/24"
  }
  ```
- Инициализуйте OSD:
  - Только SSD или только HDD: `vitastor-disk prepare /dev/sdXXX [/dev/sdYYY ...]`.
    Если вы используете десктопные SSD без конденсаторов, добавьте опцию `--disable_data_fsync off`,
    чтобы оставить кэш записи диска включённым. НЕ добавляйте эту опцию, если используете
    жёсткие диски (HDD).
  - Гибридные, SSD+HDD: `vitastor-disk prepare --hybrid /dev/sdXXX [/dev/sdYYY ...]`.
    Передайте все ваши SSD и HDD скрипту в командной строке подряд, скрипт автоматически выделит
    разделы под журналы на SSD и данные на HDD. Скрипт пропускает HDD, на которых уже есть разделы
    или вообще какие-то данные, поэтому если диски непустые, сначала очистите их с помощью
    `wipefs -a`. SSD с таблицей разделов не пропускаются, но так как скрипт создаёт новые разделы
    для журналов, на SSD должно быть доступно свободное нераспределённое место.
- Вы можете менять параметры OSD в юнитах systemd или в `vitastor.conf`. Описания параметров
  смотрите в [справке по конфигурации](../config.ru.md).
- Запустите все OSD: `systemctl start vitastor.target`

## Создайте пул

Создайте пул с помощью vitastor-cli:

```
vitastor-cli create-pool testpool --pg_size 2 --pg_count 256
```

Для пулов с кодами коррекции ошибок конфигурация должна выглядеть примерно так:

```
vitastor-cli create-pool testpool --ec 2+2 --pg_count 256
```

Добавьте также опцию `--immediate_commit none`, если вы добавляли `--disable_data_fsync off`
на этапе инициализации OSD, либо если `vitastor-disk` ругался на невозможность отключения
кэша дисков.

После этого один из мониторов должен сконфигурировать PG, а OSD должны запустить их.

Если вы используете HDD-диски, то добавьте в конфигурацию пулов опцию `"block_size": 1048576`.
Также эту опцию можно добавить в /vitastor/config/global, в этом случае она будет
применяться ко всем пулам по умолчанию.

## Проверьте состояние кластера

`vitastor-cli status`

Либо же вы можете проверять состояние PG прямо в etcd: `etcdctl --endpoints=... get --prefix /vitastor/pg/state`. Все PG должны быть 'active'.

## Создайте образ

Используйте vitastor-cli ([смотрите документацию CLI здесь](../usage/cli.ru.md)):

```
vitastor-cli create -s 10G testimg
```

После этого вы можете [запускать тесты](../usage/fio.ru.md) или [вручную запускать QEMU](../usage/qemu.ru.md) с новым образом.

## Установите плагины

- [Proxmox](../installation/proxmox.ru.md)
- [OpenStack](../installation/openstack.ru.md)
- [Kubernetes CSI](../installation/kubernetes.ru.md)

## Создайте VitastorFS

Если вы хотите использовать не только блочные образы виртуальных машин или контейнеров,
а также кластерную файловую систему, то:

- [Следуйте инструкциям](../usage/nfs.ru.md#vitastorfs)
