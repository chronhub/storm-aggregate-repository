<?php

declare(strict_types=1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;

/*
 * AggregateRepository package wiring.
 *
 * Registers the AggregateRepositoryManager. Autowired, it reads the %storm.aggregates% parameter
 * set by the bundle, plus the StreamReader / DecisionAppend ports and the MessageEnricher.
 * DefaultAggregateRepository is NOT a service; it is per-aggregate by class, id, and category, and
 * is built by the manager.
 */
return static function (ContainerConfigurator $container): void {
    $services = $container->services()
        ->defaults()
        ->autowire()
        ->autoconfigure();

    $services->load('Storm\\AggregateRepository\\', dirname(__DIR__).'/')
        ->exclude([
            dirname(__DIR__).'/DefaultAggregateRepository.php',
            dirname(__DIR__).'/SnapshotRepository.php',
            dirname(__DIR__).'/Exception/',
            dirname(__DIR__).'/Schema/',
            dirname(__DIR__).'/Snapshot/Snapshot.php',
            dirname(__DIR__).'/config/',
            dirname(__DIR__).'/Tests/',
        ]);
};
