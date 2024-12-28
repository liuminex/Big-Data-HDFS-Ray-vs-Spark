## Setup local VMs

### Create the machines

Create an Ubuntu (or ubuntu server) VM in VirtualBox.

Create two clones of the VM. Make sure to select `Generate new MAC addresses` and use `Full clone` option.

### Setup their network

In Virtualbox, press `[ctrl + H]`, create new host-only network.

In the VM settings **of both machines**:
- Set `Network -> Adapter 1` to `Bridged Adapter`
- Set `Network -> Adapter 2` to `Host-only Adapter` and use the created host-only network in the `name` field.
- Select `Advanced` -> `Adapter Type` -> `Intel Desktop`
- Select `Promiscuous Mode` -> `Allow All`.

*Start the VMs*

