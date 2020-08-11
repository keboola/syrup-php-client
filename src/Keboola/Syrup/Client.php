<?php

namespace Keboola\Syrup;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Client as GuzzleClient;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

/**
 * Class Client
 * @package Keboola\Orchestrator
 */
class Client
{
    const DEFAULT_API_URL = 'https://syrup.keboola.com';
    const DEFAULT_USER_AGENT = 'Keboola Syrup PHP Client';
    const DEFAULT_BACKOFF_RETRIES = 11;

    protected $jobFinishedStates = ["cancelled", "canceled", "success", "error", "terminated"];

    /**
     * @var int Maximum delay between queries for job state
     */
    protected $maxDelay = 10;

    /**
     * @var string Name of parent component
     */
    protected $super = '';

    /**
     * @var string Actual base request URL.
     */
    private $url;

    /**
     * @var Actual base request URL for queue
     */
    private $queueUrl;

    /**
     * GuzzleClient
     */
    protected $guzzle;

    /**
     * @param int $maxRetries
     * @return \Closure
     */
    private static function createDefaultDecider($maxRetries = 3)
    {
        return function (
            $retries,
            RequestInterface $request,
            ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() > 499) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }

    /**
     * Create a client instance
     *
     * @param array $config Client configuration settings:
     *     - token: (optional) Storage API token.
     *     - runId: (optional) Storage API runId.
     *     - url: (optional) Syrup API URL to override the default (DEFAULT_API_URL).
     *     - super: (optional) Name of parent component if any.
     *     - userAgent: (optional) Custom user agent (appended to the default).
     *     - backoffMaxTries: (optional) Number of retries in case of backend error.
     *     - logger: (optional) instance of Psr\Log\LoggerInterface.
     *     - handler: (optional) instance of GuzzleHttp\HandlerStack.
     * @param callable $delay Optional custom delay method to apply (default is exponential)
     * @return Client
     * @deprecated
     */
    public static function factory(array $config = [], callable $delay = null)
    {
        return new self($config, $delay);
    }

    /**
     * Create a client instance
     *
     * @param array $config Client configuration settings:
     *     - token: (optional) Storage API token.
     *     - runId: (optional) Storage API runId.
     *     - url: (optional) Syrup API URL to override the default (DEFAULT_API_URL).
     *     - super: (optional) Name of parent component if any.
     *     - userAgent: (optional) Custom user agent (appended to the default).
     *     - backoffMaxTries: (optional) Number of retries in case of backend error.
     *     - logger: (optional) instance of Psr\Log\LoggerInterface.
     *     - handler: (optional) instance of GuzzleHttp\HandlerStack.
     * @param callable $delay Optional custom delay method to apply (default is exponential)
     * @return Client
     */
    public function __construct(array $config = [], callable $delay = null)
    {
        $token = '';

        if (!empty($config['token'])) {
            $token = $config['token'];
        }

        $apiUrl = self::DEFAULT_API_URL;
        if (!empty($config['url'])) {
            $apiUrl = $config['url'];
        }
        $this->setUrl($apiUrl);

        if (!empty($config['queueUrl'])) {
            $this->setQueueUrl($config['queueUrl']);
        } else {
            $this->setQueueUrl($apiUrl);
        }

        $runId = '';
        if (!empty($config['runId'])) {
            $runId = $config['runId'];
        }

        $userAgent = self::DEFAULT_USER_AGENT;
        if (!empty($config['userAgent'])) {
            $userAgent .= ' - ' . $config['userAgent'];
        }

        $maxRetries = self::DEFAULT_BACKOFF_RETRIES;
        if (!empty($config['backoffMaxTries'])) {
            $maxRetries = $config['backoffMaxTries'];
        }


        $this->guzzle = $this->initClient(
            $token,
            $runId,
            $userAgent,
            $maxRetries,
            $config,
            $delay
        );

        if (!empty($config['super'])) {
            $this->setSuper($config['super']);
        }
    }

    /**
     * @param $token
     * @param $runId
     * @param $userAgent
     * @param $maxRetries
     * @param array $config
     * @param callable|null $delay
     * @return GuzzleClient
     */
    protected function initClient(
        $token,
        $runId,
        $userAgent,
        $maxRetries,
        array $config = [],
        callable $delay = null
    ) {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($config['handler']) && $config['handler'] instanceof HandlerStack) {
            $handlerStack = HandlerStack::create($config['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff for cases where job detail returns error
        $handlerStack->push(Middleware::retry(
            self::createDefaultDecider($maxRetries),
            $delay
        ));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($token, $runId, $userAgent) {
                $req = $request->withHeader('User-Agent', $userAgent);
                if ($token) {
                    $req = $req->withHeader('X-StorageApi-Token', $token);
                }
                if (!$req->hasHeader('content-type')) {
                    $req = $req->withHeader('Content-type', 'application/json');
                }
                if ($runId) {
                    $req = $req->withHeader('X-KBC-RunId', $runId);
                }
                return $req;
            }
        ));

        // Set client logger
        if (isset($config['logger']) && $config['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $config['logger'],
                new MessageFormatter(
                    "{hostname} {req_header_User-Agent} - [{ts}] \"{method} {resource} {protocol}/{version}\" " .
                    "{code} {res_header_Content-Length}"
                )
            ));
        }

        // finally create the instance
        return new GuzzleClient(['base_url' => $this->url, 'handler' => $handlerStack]);
    }

    /**
     * Set parent component.
     * @param string $super Name of the parent component.
     */
    protected function setSuper($super)
    {
        $this->super = $super;
    }

    /**
     * Set request URL
     * @param string $url Base url for requests.
     */
    protected function setUrl($url)
    {
        $this->url = $url;
    }

    /**
     * @param $queueUrl
     */
    protected function setQueueUrl($queueUrl)
    {
        $this->queueUrl = $queueUrl;
    }

    /**
     * Decode a JSON response.
     * @param Response $response
     * @return array Parsed response.
     * @throws ClientException In case response cannot be read properly.
     */
    private function decodeResponse(Response $response)
    {
        $data = json_decode($response->getBody()->read($response->getBody()->getSize()), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new ClientException('Unable to parse response body into JSON: ' . json_last_error());
        }

        return $data === null ? array() : $data;
    }

    /**
     * Create a new asynchronous job.
     *
     * @param string $component Component name
     * @param array $options Available options are:
     *      - config: configuration id.
     *      - configData: configuration data.
     *      - tag: image tag to run
     * @return array Response data with job status.
     * @throws ClientException
     */
    public function createJob($component, $options = array())
    {
        return $this->createAsyncJob($component .  "/run", "POST", $options);
    }

    /**
     * @param $path
     * @param string $method
     * @param array $options
     * @return array
     * @throws ClientException
     */
    public function createAsyncJob($path, $method = "POST", $options = array())
    {
        $uriParts = [];
        if ($this->super) {
            $uriParts[] = $this->super;
        }
        $uriParts[] = $path;
        if (substr($this->url, -1) == '/') {
            $uri = $this->url . implode('/', $uriParts);
        } else {
            $uri = $this->url . '/' . implode('/', $uriParts);
        }
        if (isset($options['tag'])) {
            $uri .= '/tag/' . $options['tag'];
        }
        $body = [];
        if (isset($options['body'])) {
            $body = $options['body'];
        }
        if (isset($options['config'])) {
            $body['config'] = $options['config'];
        }
        if (isset($options['configData'])) {
            $body['configData'] = $options['configData'];
        }

        try {
            $request = new Request($method, $uri, [], json_encode($body));
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }

    /**
     * Get asynchronous job status (waiting, processing, etc.).
     *
     * @param int|string $job Job Id.
     * @return array Response data with job status.
     * @throws ClientException
     */
    public function getJob($job)
    {
        if (substr($this->queueUrl, -1) == '/') {
            $uri = $this->queueUrl . "queue/job/{$job}";
        } else {
            $uri = $this->queueUrl . "/queue/job/{$job}";
        }
        try {
            $request = new Request('GET', $uri);
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }

    /**
     * Run a syrup job and wait for the result.
     *
     * @param string $component Component name.
     * @param array $options Available options are:
     *      - config: configuration id.
     *      - configData: configuration data.
     *      - tag: image tag to run
     * @return array Response data with job result.
     * @throws ClientException In case creation or waiting for the job failed.
     */
    public function runJob($component, array $options = [])
    {
        return $this->runAsyncAction($component . "/run", "POST", $options);
    }

    /**
     * Run  docker runner sync action
     *
     * @param string $syncActionBaseUrl
     * @param string $component
     * @param string $action
     * @param array $configData
     * @return array Sync action response
     * @throws ClientException In case of execution error
     */
    public function runSyncAction($syncActionBaseUrl, $component, $action, array $configData)
    {
        $uriParts = [];
        if ($this->super) {
            $uriParts[] = $this->super;
        }
        $uriParts[] = $component;
        $uriParts[] = 'action';
        $uriParts[] = $action;

        $uri = rtrim($syncActionBaseUrl, '/') . '/' . implode('/', $uriParts);

        $body = [
            'configData' => $configData,
        ];

        try {
            $request = new Request('POST', $uri, [], json_encode($body));
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }

    /**
     * @param $path
     * @param string $method
     * @param array $options
     * @return array
     * @throws ClientException
     */
    public function runAsyncAction($path, $method = "POST", array $options = [])
    {
        $response = $this->createAsyncJob($path, $method, $options);
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

    /**
     * @return array
     * @throws ClientException
     */
    public function getStats()
    {
        $uriBase = (substr($this->url, -1) === '/') ? $this->url : $this->url .= '/';
        try {
            $request = new Request('GET', $uriBase . 'docker/stats/project');
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }

    /**
     * @param string $componentId
     * @param string $configId
     * @param string $configVersion
     * @param ?string $variableValuesId
     * @param array $variableValuesData
     * @return array
     * @throws ClientException
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function resolveConfiguration(
        $componentId,
        $configId,
        $configVersion,
        $variableValuesId = null,
        $variableValuesData = []
    ) {
        $uriBase = (substr($this->url, -1) === '/') ? $this->url : $this->url .= '/';
        $body = [
            'componentId' => $componentId,
            'configId' => $configId,
            'configVersion' => $configVersion,
        ];
        if ($variableValuesId) {
            $body['variableValuesId'] = $variableValuesId;
        }
        if (!empty($variableValuesData)) {
            $body['variableValuesData'] = $variableValuesData;
        }
        try {
            $request = new Request(
                'POST',
                $uriBase . 'docker/configuration/resolve',
                [],
                json_encode($body)
            );
            $response = $this->guzzle->send($request);
        } catch (RequestException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
        return $this->decodeResponse($response);
    }
}
