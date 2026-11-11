# Relay

Native voice and screen sharing for Discord, through an Equicord plugin and a local media companion.

## Installation

Add Relay as a flake input:

```nix
relay.url = "github:greyxp1/Relay";
```

With Equicord configured through Nixcord, enable the companion and plugin:

```nix
{inputs, pkgs, ...}: {
  imports = [inputs.relay.nixosModules.default];
  services.relay.enable = true;
  programs.nixcord = {
    userPlugins.relay = inputs.relay.packages.${pkgs.stdenv.hostPlatform.system}.plugin;
    extraConfig.plugins.Relay.enabled = true;
  };
}
```

## Building

Build the companion and plugin from a local checkout:

```console
nix build
nix build .#plugin
```
