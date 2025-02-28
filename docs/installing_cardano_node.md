#### Installation

There are of course many options for installation of a Cardano-Node. 
We will use an approach that involves a composition of Docker images for the two required software components.

This is a relatively simple option which runs pre-built Docker images for both the Cardano-Node and Ogmios, 
and is both flexible in terms of configuration yet abstracts some of the complexity.

The function of Docker and the ability to create and use custom images is a topic we'll address later as it is 
an important technology to gain experience with anyway, and later on we'll use it to perform 
distributed data processing using Spark by simply submitting Docker images. However at this stage all we need 
to know is we can use it to run the required software without much set-up.

For that we just need to get a Docker engine running in our development environment. On your development 
machine this usually just means installing 'Docker Desktop' which comes with a UI for management as well 
as the 'engine' installed in the background.

 * Visit: https://docs.docker.com/engine/install
 * Follow the instructions for the package matching your CPU architecture

To confirm Docker has been installed ok, check which version is available in your terminal
```bash
docker info
# > Client.Version:    27.5.1
```

#### What can go wrong:
* If from the above check, or any other time using Docker you see `ERROR: Cannot connect to the 
Docker daemon at docker.sock. Is the docker daemon running?` => You likely just need to open Docker Desktop or start the Docker engine.

Now we need to run Cardano-Node and Ogmios Docker images. We clone the repository 
from the makers of Ogmios, just to get the configs and the `docker-compose.yaml` 
file which contains configurations for tested and version controlled releases for 
Cardano-Node and Ogmios.

From your normal working directory, such as `~/Workspace`

```bash
git clone --recursive --depth 1 git@github.com:cardanosolutions/ogmios.git
cd ogmios
```

Now before going ahead, review the `docker-compose.yaml` in this *ogmios* directory which contains all
the necessary configs and features of our Cardano-Node application.

We notice in the file it has volume mounts
```yaml
    volumes:
      - ./server/config/network/${NETWORK:-mainnet}/cardano-node:/config
      - ./server/config/network/${NETWORK:-mainnet}/genesis:/genesis
      - node-db:/data
      - node-ipc:/ipc
```

This means that *within the current directory* of the newly cloned *ogmios* repo, we expect some 
configs that become mapped from the LHS directory to the RHS directory inside the container when it is 
running. The `--recursive` flag used above made sure to include the correct configs, otherwise they can
be manually copied from the official configs at https://github.com/input-output-hk/cardano-configurations.git.

And also notice that we don't have any node-db or node-ipc directories within our local ogmios root directory. 
Those are Docker named-volumes, meaning docker will maintain and persist them to be reused across applications, 
and are where the raw ledger data and a node socket will be stored respectively. 
There is an alternate option to bind a local directory using a similar method as used for the configs here. e.g. `./server..`, 
however we'll stick with named-volumes for now.

Now, make sure the Docker Engine is running, and we should be good to fire it up and monitor the logs
```bash
docker-compose up -d
docker-compose logs cardano-node -f
# > 
# cardano-node-1  | [1ba0ebdf:cardano.node.ChainDB:Notice:38] [2025-01-14 05:13:56.28 UTC] Chain extended, new tip: f9b821a4cb5c5326d83d4ee3cd46b4cda071534483a310c791733dcdeb8546e5 at slot 1948572
# cardano-node-1  | [1ba0ebdf:cardano.node.ChainDB:Notice:38] [2025-01-14 05:13:57.63 UTC] Chain extended, new tip: f2519e2c7f9d82876a49e648ce5accf2c09c8306106812c2be22ce093f330f77 at slot 1949155
# ...
```

And we can stop with
```bash
docker-compose down
```

. . .

### More ways to install your own node

**Run a bundled Docker image**

This is a slightly simpler method that runs an image which has both the Cardano-Node and Ogmios bundled together.
It is a secondary preference partly because it's a little bit less configurable (or requires rebuilding the Docker image with customisations),
but for me mainly because there is currently no built and available image from the public Docker repo for the Apple Mac CPU architecture. 
This would also require rebuilding the image yourself. However if you don't have this limitation, and just want to get started as quick 
as possible, this is a good option. 

```bash
docker pull cardanosolutions/cardano-node-ogmios:latest
docker run -it \
  --name cardano-node-ogmios \
  -p 1337:1337 \
  -v cardano-node-ogmios-mainnet-db:/db \
  cardanosolutions/cardano-node-ogmios:latest
```

**Build and run native software**

Building the two required software components from source code, custom for the CPU architecture you are using, is
another option which is perfectly fine, however takes a bit more care and a few more steps. 
You might prefer this approach if you want to avoid using Docker, or as an upgrade later to get more control,
customisation or optimisation.

The steps broadly include having a Haskell build environment with all the necessary libraries 
(e.g. some non-standard cryptographic libraries) available, then building and running 
https://github.com/IntersectMBO/cardano-node according to their instructions 
as well as building and running https://github.com/CardanoSolutions/ogmios and making sure 
the two software components are connected over the network. 
You also would need to ensure the two versions are compatible. We'll leave it at that, 
it's not too daunting, you can follow the official or other public guides.