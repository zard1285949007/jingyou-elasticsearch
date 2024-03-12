<?php

declare(strict_types=1);

namespace Liguizhou\Elasticsearch;

use Hyperf\Utils\Collection;

/**
 * Class Builder
 * @package Elasticsearch
 * Date : 2023/7/17
 * Author: lgz
 * Desc: ElasticSearch 链式操作
 */
trait ExcuteBuilder
{
    /**
     * 执行原始方法
     * @param $body
     * @param $method
     * @return mixed|null
     * @throws \Exception
     */
    public function originClient($body, $method)
    {
        $this->sql = $body;
        return $this->run($method);
    }

    /**
     * 单条插入
     * @param array $value
     * @param array $fields
     * @return bool
     * @throws \Exception
     */
    public function insert(array $value)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'type'  => '_doc',
            'body'  => $value,
        ];
        $this->sql = $body;

        return $this->run('index');
    }

    /**
     * 批量插入
     * @param array $values
     * @param array $fields
     * @return Collection|\Illuminate\Support\Collection
     * @throws \Exception
     */
    public function batchInsert(array $insertData)
    {
        $body = [];
        foreach ($insertData as $key => $value) {
            $indexData = [
                'index' => ['_index' => $this->model->getIndex(), '_id' => $key],
            ];

            $body['body'][] = $indexData;
            $body['body'][] = $value;
        }

        $this->sql = $body;
        $result = $this->run('bulk');
        $collection = collect($result['items'])->map(function ($value, $key) use ($insertData) {
            $model = $this->model->newInstance();
            $model->setOriginal($value);
            $model->setAttributes($value);
            return $model;
        });

        return $collection;
    }

    public function batchUpdateOrInsert(array $insertData, $updateFields = [], $useKey=true)
    {
        $body = [];
        foreach ($insertData as $key => $value) {
            $indexData = [
                'update' => ['_index' => $this->model->getIndex()],
            ];
            if ($useKey) {
                $indexData['update']['_id'] = $key;
            }

            $body['body'][] = $indexData;
            $updateData = empty($updateFields) ? $value : array_intersect_key($value, array_flip($updateFields));
            $body['body'][] = ['doc' => $updateData, 'upsert' => $value];
        }

        $this->sql = $body;
        $result = $this->run('bulk');
        $collection = collect($result['items'])->map(function ($value, $key) {
            $model = $this->model->newInstance();
            $model->setOriginal($value);
            $model->setAttributes($value);
            return $model;
        });

        return $collection;
    }

    /**
     * 更新插入数据
     * @param $id
     * @param array $updateData
     * @param array $insertData
     * @return mixed|null
     * @throws \Exception
     */
    public function updateOrInsert($id, array $insertData, array $updateFields = [])
    {
        $updateData = empty($updateFields) ? $insertData : array_intersect_key($insertData, array_flip($updateFields));
        $body = [
            'index' => $this->model->getIndex(),
            'type'  => '_doc',
            'id'    => $id,
            'body'  => [
                'doc'    => $updateData,
                'upsert' => $insertData,
            ]
        ];

        $this->sql = $body;

        return $this->run('update');
    }

    /**
     * 单条更新
     * @param $id
     * @param array $value
     * @return mixed|null
     * @throws \Exception
     */
    public function update($id, array $value)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'type'  => '_doc',
            'id'    => $id,
            'body'  => [
                'doc' => $value,
            ]
        ];

        $this->sql = $body;

        return $this->run('update');
    }

    public function updateByQuery(array $body)
    {
        $query = $this->parseQuery();
        if (empty($query)) { //先不允许全更新
            return [];
        }
        $source = '';
        foreach ($body as $key => $value) {
            $source .= "ctx._source['" . $key . "']=params['" . $key . "'];";
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                'query'  => $query,
                'script' => [
                    'source' => $source,
                    'params' => $body
                ]
            ]
        ];

        $this->sql = $body;

        return $this->run('updateByQuery');
    }

    public function existsIndex()
    {
        $body = [
            'index' => $this->model->getIndex(),
        ];

        $this->sql = $body;

        return $this->run('indices.exists');
    }

    public function createIndex(array $values, array $settings = [], $alias = '')
    {
        $properties = [];
        foreach ($values as $key => $value) {
            if (is_array($value)) {
                $properties[$key] = $value;
            } else {
                $properties[$key] = ['type' => $value];
            }
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                'mappings' => [
                    '_source'    => [
                        'enabled' => true
                    ],
                    'properties' => $properties
                ]
            ]
        ];

        if (!empty($settings)) {
            $body['body']['settings'] = $settings;
        } else {
            $body['body']['settings'] = [
                'number_of_shards'   => 1,
                'number_of_replicas' => 1
            ];
        }

        if (!empty($alias)) {
            $body['body']['aliases'] = [$alias => new \stdClass()];
        }

        $this->sql = $body;

        return $this->run('indices.create');
    }

    public function updateIndex(array $values)
    {
        $properties = [];
        foreach ($values as $key => $value) {
            if (is_array($value)) {
                $properties[$key] = $value;
            } else {
                $properties[$key] = ['type' => $value];
            }
        }

        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                '_source'    => [
                    'enabled' => true
                ],
                'properties' => $properties
            ]
        ];

        $this->sql = $body;

        return $this->run('indices.putMapping');
    }

    public function deleteIndex()
    {
        $body = [
            'index' => $this->model->getIndex(),
        ];

        $this->sql = $body;

        return $this->run('indices.delete');
    }

    public function delete($id)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'id'    => $id
        ];
        $this->sql = $body;
        return $this->run('delete');
    }

    public function deleteByQuery()
    {
        $query = $this->parseQuery();
        if (empty($query)) { //先不允许全删除
            return [];
        }
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => [
                'query' => $query
            ]
        ];
        $this->sql = $body;
        return $this->run('deleteByQuery');
    }

    public function updateSetting($value)
    {
        $body = [
            'index' => $this->model->getIndex(),
            'body'  => $value
        ];

        $this->sql = $body;
        return $this->run('indices.putSettings');
    }

    public function updateClusterSetting($value)
    {
        $body = [
            'body' => $value

        ];

        $this->sql = $body;

        return $this->run('cluster.putSettings');
    }

    public function getSetting()
    {
        $body = [
            'index' => $this->model->getIndex()
        ];
        $this->sql = $body;
        return $this->run('indices.getSettings');
    }
}
