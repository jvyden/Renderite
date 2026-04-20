# What is this?
This repository contains the core libraries that Resonite uses to interact with the Unity renderer. Resonite is a free social VR sandbox platform, which allows for socialization and collaborative in-game building.

You can get Resonite free on Steam: https://store.steampowered.com/app/2519830/Resonite/

Resonite uses a unique multi-process architecture, where the majority of Resonite is a fully custom engine running with modern .NET 10 runtime. The engine communicates with the Unity Renderer process via IPC (inter process communication) and shared memory - these are the libraries that faciliate that.

This architecture allows for much higher performance and properly isolates the renderer from the rest of the engine, allowing it to be replaced in the future.

## Renderite.Shared
This library contains shared classes and models for the IPC communication that are not tied to Unity specifically. If you want to make a custom renderer for Resonite, you can use this library, while ignoring Renderite.Unity completely.

This library is also referenced by FrooxEngine itself - it provides the shared resources between the two.

## Renderite.Unity
This library contains Unity specific classes and behaviors that handle most interfacing with the Unity engine to render things.

This library is used in the Unity renderer project only and majority of the actual rendering logic is in this library.

# Unity Renderer Project
The Unity side of the renderer that uses these libraries is available here:
https://github.com/Yellow-Dog-Man/Renderite.Unity.Renderer

# Contributing
> [!IMPORTANT]
> If you'd like to contribute and learn more, please read the guidelines in the Unity Renderer repo above!
>
> This repository follows the same rules and guidelines.
