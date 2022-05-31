# Syrup PHP Client [![Build Status](https://travis-ci.org/keboola/syrup-php-client.svg?branch=master)](https://travis-ci.org/keboola/syrup-php-client) [![Code Climate](https://codeclimate.com/github/keboola/syrup-php-client/badges/gpa.svg)](https://codeclimate.com/github/keboola/syrup-php-client) [![Test Coverage](https://codeclimate.com/github/keboola/syrup-php-client/badges/coverage.svg)](https://codeclimate.com/github/keboola/syrup-php-client)

Simple PHP wrapper library for Syrup async calls.

## Installation

Library is available as composer package.
To start using composer in your project follow these steps:

**Install composer**

```bash
curl -s http://getcomposer.org/installer | php
mv ./composer.phar ~/bin/composer # or /usr/local/bin/composer
```

**Create composer.json file in your project root folder:**
```json
{
    "require": {
        "php" : ">=5.4.0",
        "keboola/syrup-php-client": "2.0.*"
    }
}
```

**Install package:**

```bash
composer install
```

**Add autoloader in your bootstrap script:**

```php
require 'vendor/autoload.php';
```

Read more in [Composer documentation](http://getcomposer.org/doc/01-basic-usage.md)

## Usage examples

Create and run a job

```php
require 'vendor/autoload.php';

use Keboola\Syrup\Client,

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'runId' => 'CURRENT_RUNID'
]);

$response = $client->runJob("test-component", array("config" => 1));
```

Encrypt component and project specific string for a Docker component (calls `docker/test-component/configs/encrypt`)

```php
require 'vendor/autoload.php';

use Keboola\Syrup\Client,

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'runId' => 'CURRENT_RUNID',
  'super' => 'docker'
]);

$response = $client->encryptString("test-component", "string", ["path" => "configs"]);
```

Create a custom job on a component (calls `POST provisioning/async/docker`) with `{"type" => "rstudio"}` body

```php
require 'vendor/autoload.php';

use Keboola\Syrup\Client,

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'runId' => 'CURRENT_RUNID',
  'super' => 'provisioning'
]);

$response = $client->runAsyncAction("async/docker", "POST", ["body" => ["type" => "rstudio"]]);
```

Create a custom DELETE job on a component (calls `DELETE provisioning/async/docker/1`)
                                         
```php
require 'vendor/autoload.php';

use Keboola\Syrup\Client,

$client = new Client([
  'token' => 'YOUR_TOKEN',
  'runId' => 'CURRENT_RUNID',
  'super' => 'provisioning'
]);

$response = $client->runAsyncAction("async/docker/1", "DELETE");
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
