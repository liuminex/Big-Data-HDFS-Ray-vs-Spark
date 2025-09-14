## Setup local VMs

### Create the machines

Create an Ubuntu (or other debian based distribution) VM in VirtualBox.

Create two clones of the VM. Make sure to select `Generate new MAC addresses` and use `Full clone` option.

### Network Adapter Setup

In Virtualbox, press `[ctrl + H]`, create new host-only network.

In the VM settings **of all machines**:
- Set `Network -> Adapter 1` to `NAT`
- Set `Network -> Adapter 2` to `Host-only Adapter` and use the created host-only network in the `name` field. Also, in Adapter 2:
    - Select `Advanced` -> `Adapter Type` -> `Intel Desktop`
    - Select `Promiscuous Mode` -> `Allow All`.

*Start the VMs*

