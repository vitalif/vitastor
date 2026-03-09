# Vitastor

[Read English version](README.md)

## Идея

Вернём былую скорость кластерному блочному хранилищу!

Vitastor - распределённая блочная, файловая и объектная SDS (программная СХД), прямой аналог Ceph RBD, CephFS и RGW,
а также внутренних СХД популярных облачных провайдеров. Однако, в отличие от них, Vitastor
быстрый и при этом простой. Только пока маленький :-).

Vitastor архитектурно похож на Ceph, что означает атомарность и строгую консистентность,
репликацию через первичный OSD, симметричную кластеризацию без единой точки отказа
и автоматическое распределение данных по любому числу дисков любого размера с настраиваемыми схемами
избыточности - репликацией или с произвольными кодами коррекции ошибок.

Vitastor нацелен в первую очередь на SSD и SSD+HDD кластеры с как минимум 10 Гбит/с сетью, поддерживает
TCP и RDMA и на хорошем железе может достигать задержки 4 КБ чтения и записи на уровне ~0.1 мс,
что примерно в 10 раз быстрее, чем Ceph и другие популярные программные СХД.

Vitastor поддерживает QEMU-драйвер, протоколы UBLK, NBD и NFS, драйверы OpenStack, OpenNebula, Proxmox, Kubernetes.
Другие драйверы могут также быть легко реализованы.

Подробности смотрите в документации по ссылкам. Можете начать отсюда: [Быстрый старт](docs/intro/quickstart.ru.md).

## Презентации и записи докладов

- KuberConf'2025: [видео](https://vitastor.io/presentation/kuberconf.webm)
- Highload'2025: [видео](https://vitastor.io/presentation/hl2025/hl2025.webm),
  [на youtube](https://www.youtube.com/watch?v=0R8MLjFtz7g), презентация
  ([на русском](https://vitastor.io/presentation/hl2025/), [на английском](https://vitastor.io/presentation/hl2025/en.html))
- Highload'2022: презентация ([на русском](https://vitastor.io/presentation/highload/highload.html)),
  [видео](https://vitastor.io/presentation/highload/talk.webm)
- DevOpsConf'2021: презентация ([на русском](https://vitastor.io/presentation/devopsconf/devopsconf.html),
  [на английском](https://vitastor.io/presentation/devopsconf/devopsconf_en.html)),
  [видео](https://vitastor.io/presentation/devopsconf/talk.webm)

## Документация

- Введение
  - [Быстрый старт](docs/intro/quickstart.ru.md)
  - [Возможности](docs/intro/features.ru.md)
  - [Архитектура](docs/intro/architecture.ru.md)
  - [Автор и лицензия](docs/intro/author.ru.md)
- Установка
  - [Пакеты](docs/installation/packages.ru.md)
  - [Docker](docs/installation/docker.ru.md)
  - [Proxmox](docs/installation/proxmox.ru.md)
  - [OpenNebula](docs/installation/opennebula.ru.md)
  - [OpenStack](docs/installation/openstack.ru.md)
  - [Kubernetes CSI](docs/installation/kubernetes.ru.md)
  - [S3](docs/installation/s3.ru.md)
  - [Сборка из исходных кодов](docs/installation/source.ru.md)
- Конфигурация
  - [Обзор](docs/config.ru.md)
  - Параметры
    - [Общие](docs/config/common.ru.md)
    - [Сетевые](docs/config/network.ru.md)
    - [Клиентский код](docs/config/client.ru.md)
    - [Глобальные дисковые параметры](docs/config/layout-cluster.ru.md)
    - [Дисковые параметры OSD](docs/config/layout-osd.ru.md)
    - [Прочие параметры OSD](docs/config/osd.ru.md)
    - [Параметры мониторов](docs/config/monitor.ru.md)
    - [Безопасность](docs/config/security.ru.md)
  - [Настройки пулов](docs/config/pool.ru.md)
  - [Метаданные образов в etcd](docs/config/inode.ru.md)
- Использование
  - [vitastor-cli](docs/usage/cli.ru.md) (консольный интерфейс)
  - [vitastor-disk](docs/usage/disk.ru.md) (управление дисками)
  - [fio](docs/usage/fio.ru.md) для тестов производительности
  - [UBLK](docs/usage/ublk.ru.md) для монтирования ядром
  - [NBD](docs/usage/nbd.ru.md) - старый интерфейс для монтирования ядром
  - [QEMU, qemu-img и VDUSE](docs/usage/qemu.ru.md)
  - [NFS](docs/usage/nfs.ru.md) кластерная файловая система и псевдо-ФС прокси
  - [Безопасность](docs/usage/security.ru.md)
  - [Администрирование](docs/usage/admin.ru.md)
- Производительность
  - [Понимание сути производительности](docs/performance/understanding.ru.md)
  - [Теоретический максимум](docs/performance/theoretical.ru.md)
  - [Пример сравнения с Ceph](docs/performance/comparison1.ru.md)
  - [Более новый тест Vitastor 1.3.1](docs/performance/bench2.ru.md)

## Автор и лицензия

Автор: Виталий Филиппов (vitalif [at] yourcmc.ru), 2019+

Заходите в Telegram-чат Vitastor: https://t.me/vitastor

Лицензия: VNPL 1.1 на серверный код и двойная VNPL 1.1 + GPL 2.0+ на клиентский.

VNPL - "сетевой копилефт", собственная свободная копилефт-лицензия
Vitastor Network Public License 1.1, основанная на GNU GPL 3.0 с дополнительным
условием "Сетевого взаимодействия", требующим распространять все программы,
специально разработанные для использования вместе с Vitastor и взаимодействующие
с ним по сети, под лицензией VNPL или под любой другой свободной лицензией.

Идея VNPL - расширение действия копилефта не только на модули, явным образом
связываемые с кодом Vitastor, но также на модули, оформленные в виде микросервисов
и взаимодействующие с ним по сети.

Таким образом, если вы хотите построить на основе Vitastor сервис, содержаший
компоненты с закрытым кодом, взаимодействующие с Vitastor, вам нужна коммерческая
лицензия от автора 😀.

На Windows и любое другое ПО, не разработанное *специально* для использования
вместе с Vitastor, никакие ограничения не накладываются.

Клиентские библиотеки распространяются на условиях двойной лицензии VNPL 1.0
и также на условиях GNU GPL 2.0 или более поздней версии. Так сделано в целях
совместимости с таким ПО, как QEMU и fio.

Вы можете найти полный текст VNPL 1.1 на английском языке в файле [VNPL-1.1.txt](VNPL-1.1.txt),
VNPL 1.1 на русском языке в файле [VNPL-1.1-RU.txt](VNPL-1.1-RU.txt), а GPL 2.0 в файле [GPL-2.0.txt](GPL-2.0.txt).
