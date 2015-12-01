<?php
namespace Keboola\Syrup;

use Guzzle\Common\Collection;
use Guzzle\Plugin\Backoff\BackoffPlugin;
use Guzzle\Service\Description\ServiceDescription;

/**
 * Class Client
 * @package Keboola\Orchestrator
 */
class Client extends \Guzzle\Service\Client
{
    const DEFAULT_API_URL = 'https://syrup.keboola.com';

    protected $jobFinishedStates = array("cancelled", "canceled", "success", "error", "terminated");

    protected $maxDelay = 10;

    /**
     * @var string parent component, eg https://syrup.keboola.com/docker
     */
    protected $super = '';

    /**
     * @param array $config
     * @return Client
     */
    public static function factory($config = array())
    {
        $default = array(
            'url' => static::DEFAULT_API_URL,
        );
        $required = array('token');
        $config = Collection::fromConfig($config, $default, $required);
        $config['curl.options'] = array(
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_0,
        );

        $config['request.options'] = array(
            'headers' => array(
                'X-StorageApi-Token' => $config->get('token'),
                'X-KBC-RunId' => $config->get('runId')
            )
        );

        $client = new static($config->get('url'), $config);

        // Attach a service description to the client
        $description = ServiceDescription::factory(__DIR__ . '/service.json');
        $client->setDescription($description);
        $client->setBaseUrl($config->get('url'));

        if ($config->get("super")) {
            $client->super = $config->get("super");
        }

        return $client;
    }

    /**
     * Create a new async job
     *
     * Available options are
     *  - config - configuration id
     *  - configData - configuration data
     *
     * @param $component
     * @param array $options
     * @return mixed
     */
    public function createJob($component, $options = array())
    {
        $params = $options;
        $params["component"] = $component;
        $params["super"] = $this->super;
        $command = $this->getCommand('CreateJob', $params);
        return $command->execute();
    }

    /**
     *
     * Get async job status
     *
     * @param $job
     * @return mixed
     */
    public function getJob($job)
    {
        $params = array(
            "job" => $job
        );
        return $this->getCommand('GetJob', $params)->execute();
    }

    /**
     *
     * Encrypt string
     *
     * Options array can contain `path` attribute to allow further level nesting, eg.
     * .../docker/docker-config-encrypt-verify/configs/encrypt
     *     ^ super ^ component                 ^ path
     * @param $component
     * @param $string
     * @param array $options
     * @return mixed
     */
    public function encryptString($component, $string, $options = [])
    {
        $options["component"] = $component;
        $options["body"] = $string;
        $command = $this->getCommand('Encrypt', $options);
        $command->prepare();
        $command->getRequest()->addHeader("Content-Type", "text/plain");
        return $command->execute();
    }

    /**
     *
     * Encrypt array
     *
     * Options array can contain `path` attribute to allow further level nesting, eg.
     * .../docker/docker-config-encrypt-verify/configs/encrypt
     *     ^ super ^ component                 ^ path
     *
     * @param $component
     * @param $array
     * @param array $options
     * @return mixed
     */
    public function encryptArray($component, $array, $options = [])
    {
        $options["component"] = $component;
        $options["body"] = json_encode($array);
        $command = $this->getCommand('Encrypt', $options);
        $command->prepare();
        $command->getRequest()->addHeader("Content-Type", "application/json");
        return $command->execute();
    }


    /**
     * @param $component
     * @param array $options
     * @return mixed
     * @throws ClientException
     */
    public function runJob($component, $options = array())
    {
        $response = $this->createJob($component, $options);
        if (!isset($response["id"])) {
            throw new ClientException("Invalid response.");
        }
        $finished = false;
        $attempt = 0;
        $job = array();
        while (!$finished) {
            $job = $this->getJob($response["id"]);
            if (in_array($job["status"], $this->jobFinishedStates)) {
                $finished = true;
            }
            $attempt++;
            sleep(min(pow(2, $attempt), $this->maxDelay));
        }
        return $job;
    }
}
