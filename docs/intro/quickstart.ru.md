[Документация](../../README-ru.md#документация) → Введение → Быстрый старт

-----

[Read in English](quickstart.en.md)

# Быстрый старт

- [Подготовка](#подготовка)
- [Настройте мониторы](#настройте-мониторы)
- [Настройте OSD](#настройте-osd)
- [Создайте пул](#создайте-пул)
- [Проверьте состояние кластера](#проверьте-состояние-кластера)
- [Создайте образ](#создайте-образ)
- [Установите плагины](#установите-плагины)

## Подготовка

- Возьмите серверы с SSD (SATA или NVMe), желательно с конденсаторами (серверные SSD). Можно
  использовать и десктопные SSD, включив режим отложенного fsync, но производительность будет хуже.
  О конденсаторах читайте [здесь](../config/layout-cluster.ru.md#immediate_commit).
- Возьмите быструю сеть, минимум 10 гбит/с. Идеал - что-то вроде Mellanox ConnectX-4 с RoCEv2.
- Для лучшей производительности отключите энергосбережение CPU: `cpupower idle-set -D 0 && cpupower frequency-set -g performance`.
- [Установите пакеты Vitastor](../installation/packages.ru.md).

## Настройте мониторы

На хостах, выделенных под мониторы:
- Пропишите одинаковые etcd_address в `/etc/vitastor/vitastor.conf`. Например:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"]
  }
  ```
- Инициализируйте сервисы etcd, запустив `/usr/lib/vitastor/mon/make-etcd`
- Запустите etcd и мониторы: `systemctl enable --now etcd vitastor-mon`

## Настройте OSD

- Пропишите etcd_address и osd_network в `/etc/vitastor/vitastor.conf`. Например:
  ```
  {
    "etcd_address": ["10.200.1.10:2379","10.200.1.11:2379","10.200.1.12:2379"],
    "osd_network": "10.200.1.0/24"
  }
  ```
- Инициализуйте OSD:
  - SSD: `vitastor-disk prepare /dev/sdXXX [/dev/sdYYY ...]`
  - Гибридные, SSD+HDD: `vitastor-disk prepare --hybrid /dev/sdXXX [/dev/sdYYY ...]`.
    Передайте все ваши SSD и HDD скрипту в командной строке подряд, скрипт автоматически выделит
    разделы под журналы на SSD и данные на HDD. Скрипт пропускает HDD, на которых уже есть разделы
    или вообще какие-то данные, поэтому если диски непустые, сначала очистите их с помощью
    `wipefs -a`. SSD с таблицей разделов не пропускаются, но так как скрипт создаёт новые разделы
    для журналов, на SSD должно быть доступно свободное нераспределённое место.
- Вы можете менять параметры OSD в юнитах systemd или в `vitastor.conf`. Описания параметров
  смотрите в [справке по конфигурации](../config.ru.md).
- Если все ваши диски - серверные с конденсаторами, пропишите это в глобальную конфигурацию в etcd: \
  `etcdctl --endpoints=... put /vitastor/config/global '{"immediate_commit":"all"}'`
- Запустите все OSD: `systemctl start vitastor.target`

## Создайте пул

Создайте конфигурацию пула с помощью etcdctl:

```
etcdctl --endpoints=... put /vitastor/config/pools '{"1":{"name":"testpool",
  "scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":256,"failure_domain":"host"}}'
```

Для пулов с кодами коррекции ошибок конфигурация должна выглядеть примерно так:

```
etcdctl --endpoints=... put /vitastor/config/pools '{"2":{"name":"ecpool",
  "scheme":"ec","pg_size":4,"parity_chunks":2,"pg_minsize":2,"pg_count":256,"failure_domain":"host"}`
```

После этого один из мониторов должен сконфигурировать PG, а OSD должны запустить их.

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
