[Документация](../../README-ru.md#документация) → Установка → Proxmox

-----

[Read in English](proxmox.en.md)

# Proxmox

Чтобы подключить Vitastor к Proxmox Virtual Environment (поддерживаются версии 6.4-7.3):

- Добавьте соответствующий Debian-репозиторий Vitastor в sources.list на хостах Proxmox:
  buster для 6.4, bullseye для 7.3, pve7.1 для 7.1, pve7.2 для 7.2
- Установите пакеты vitastor-client, pve-qemu-kvm, pve-storage-vitastor (* или см. сноску) из репозитория Vitastor
- Определите тип хранилища в `/etc/pve/storage.cfg` (см. ниже)
- Обязательно заблокируйте доступ от виртуальных машин к сети Vitastor (OSD и etcd), т.к. Vitastor (пока) не поддерживает аутентификацию
- Перезапустите демон Proxmox: `systemctl restart pvedaemon`

Пример `/etc/pve/storage.cfg` (единственная обязательная опция - vitastor_pool, все остальные
перечислены внизу для понимания значений по умолчанию):

```
vitastor: vitastor
    # Пул, в который будут помещаться образы дисков
    vitastor_pool testpool
    # Путь к файлу конфигурации
    vitastor_config_path /etc/vitastor/vitastor.conf
    # Адрес(а) etcd, нужны, только если не указаны в vitastor.conf
    vitastor_etcd_address 192.168.7.2:2379/v3
    # Префикс ключей метаданных в etcd
    vitastor_etcd_prefix /vitastor
    # Префикс имён образов
    vitastor_prefix pve/
    # Монтировать образы через NBD прокси, через ядро (нужно только для контейнеров)
    vitastor_nbd 0
```

\* Примечание: вместо установки пакета pve-storage-vitastor вы можете вручную скопировать файл
[patches/VitastorPlugin.pm](patches/VitastorPlugin.pm) на хосты Proxmox как
`/usr/share/perl5/PVE/Storage/Custom/VitastorPlugin.pm`.
