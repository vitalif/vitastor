[Documentation](../../README.md#documentation) → Introduction → Author and License

-----

[Читать на русском](author.ru.md)

# Author and License

Copyright (c) Vitaliy Filippov (vitalif [at] yourcmc.ru), 2019+

Join Vitastor Telegram Chat: https://t.me/vitastor

License: VNPL 1.1 for server-side code and dual VNPL 1.1 + GPL 2.0+ for client tools.

Server-side code is licensed only under the terms of VNPL.

Client libraries (cluster_client and so on) are dual-licensed under the same
VNPL 1.1 and also GNU GPL 2.0 or later to allow for compatibility with GPLed
software like QEMU and fio.

## VNPL

Vitastor Network Public License 1.1 (VNPL 1.1) is a copyleft license based on
GNU GPLv3.0 with the additional "Network Interaction" clause which requires
opensourcing all programs directly or indirectly interacting with Vitastor
through a computer network and expressly designed to be used in conjunction
with it ("Proxy Programs"). Proxy Programs may be made public not only under
the terms of the same license, but also under the terms of any GPL-Compatible
Free Software License, as listed by the Free Software Foundation.
This is a stricter copyleft license than the Affero GPL.

The idea of VNPL is, in addition to modules linked to Vitastor code in a single
binary file, to extend copyleft action to micro-service modules only interacting
with it over the network.

Basically, you can't use the software in a proprietary environment to provide
its functionality to users without opensourcing all intermediary components
standing between the user and Vitastor or purchasing a commercial license
from the author 😀.

At the same time, VNPL doesn't impose any restrictions on software *not specially designed*
to be used with Vitastor, for example, on Windows running inside a VM with a Vitastor disk.

## Explanation

Network copyleft is governed by the clause **13. Remote Network Interaction** of VNPL.

A program is considered to be a "Proxy Program" if it meets both conditions:
- It is specially designed to be used with Vitastor. Basically, it means that the program
  has any functionality specific to Vitastor and thus "knows" that it works with Vitastor,
  not with something random.
- It interacts with Vitastor directly or indirectly through any programming interface,
  including API, CLI, network or any wrapper (also considered a Proxy Program itself).

If, in addition to that:
- You give any user an apportunity to interact with Vitastor directly or indirectly through
  any computer interface including the network or any number of wrappers (Proxy Programs).

Then VNPL requires you to publish the code of all above Proxy Programs to all above users
under the terms of any GPL-compatible license - that is, GPL, LGPL, MIT/BSD or Apache 2,
because "GPL compatibility" is treated as an ability to legally include licensed code in
a GPL application.

So, if you have a "Proxy Program", but it's not open to the user who directly or indirectly
interacts with Vitastor - you are forbidden to use Vitastor under the terms of VNPL and you
need a commercial license which doesn't contain open-source requirements.

## Examples

- Vitastor Kubernetes CSI driver which creates PersistentVolumes by calling `vitastor-cli create`.
  - Yes, it interacts with Vitastor through vitastor-cli.
  - Yes, it is designed specially for use with Vitastor (it has no sense otherwise).
  - So, CSI driver **definitely IS** a Proxy Program and must be published under the terms of
    a free software license.
- Windows, installed in a VM with the system disk on Vitastor storage.
  - Yes, it interacts with Vitastor indirectly - it reads and writes data through the block
    device interface, emulated by QEMU.
  - No, it definitely isn't designed specially for use with Vitastor - Windows was created long
    ago before Vitastor and doesn't know anything about it.
  - So, Windows **definitely IS NOT** a Proxy Program and VNPL doesn't require to open it.
- Cloud control panel which makes requests to Vitastor Kubernetes CSI driver.
  - Yes, it interacts with Vitastor indirectly through the CSI driver, which is a Proxy Program.
  - May or may not be designed specially for use with Vitastor. How to determine exactly?
    Imagine that Vitastor is replaced with any other storage (for example, with a proprietary).
    Do control panel functions change in any way? If they do (for example, if snapshots stop working),
    then the panel contains specific functionality and thus is designed specially for use with Vitastor.
    Otherwise, the panel is universal and isn't designed specially for Vitastor.
  - So, whether you are required to open-source the panel also **depends** on whether it
    contains specific functionality or not.

## Why?

Because I believe into the spirit of copyleft (Linux wouldn't became so popular without GPL!)
and, at the same time, I want to have a way to monetize the product.

Existing licenses including AGPL are useless for it with an SDS - SDS is a very deeply
internal software which is almost definitely invisible to the user and thus AGPL doesn't
require anyone to open the code even if they make a proprietary fork.

And, in fact, the current situation in the world where GPL is though to only restrict direct
linking of programs into a single executable file, isn't much correct. Nowadays, programs
are more often linked with network API calls, not with /usr/bin/ld, and a software product
may consist of dozens of microservices interacting with each other over the network.

That's why we need VNPL to keep the license sufficiently copyleft.

## License Texts

- VNPL 1.1 in English: [VNPL-1.1.txt](../../VNPL-1.1.txt)
- VNPL 1.1 in Russian: [VNPL-1.1-RU.txt](../../VNPL-1.1-RU.txt)
- GPL 2.0: [GPL-2.0.txt](../../GPL-2.0.txt)
