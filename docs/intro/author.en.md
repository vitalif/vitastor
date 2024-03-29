[Documentation](../../README.md#documentation) → Introduction → Author and License

-----

[Читать на русском](author.ru.md)

# Author and License

Copyright (c) Vitaliy Filippov (vitalif [at] yourcmc.ru), 2019+

Join Vitastor Telegram Chat: https://t.me/vitastor

All server-side code (OSD, Monitor and so on) is licensed under the terms of
Vitastor Network Public License 1.1 (VNPL 1.1), a copyleft license based on
GNU GPLv3.0 with the additional "Network Interaction" clause which requires
opensourcing all programs directly or indirectly interacting with Vitastor
through a computer network and expressly designed to be used in conjunction
with it ("Proxy Programs"). Proxy Programs may be made public not only under
the terms of the same license, but also under the terms of any GPL-Compatible
Free Software License, as listed by the Free Software Foundation.
This is a stricter copyleft license than the Affero GPL.

Please note that VNPL doesn't require you to open the code of proprietary
software running inside a VM if it's not specially designed to be used with
Vitastor.

Basically, you can't use the software in a proprietary environment to provide
its functionality to users without opensourcing all intermediary components
standing between the user and Vitastor or purchasing a commercial license
from the author 😀.

Client libraries (cluster_client and so on) are dual-licensed under the same
VNPL 1.1 and also GNU GPL 2.0 or later to allow for compatibility with GPLed
software like QEMU and fio.

You can find the full text of VNPL-1.1 in the file [VNPL-1.1.txt](../../VNPL-1.1.txt).
GPL 2.0 is also included in this repository as [GPL-2.0.txt](../../GPL-2.0.txt).
