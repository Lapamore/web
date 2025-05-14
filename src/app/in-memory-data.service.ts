import { Injectable } from '@angular/core';
import { InMemoryDbService } from 'angular-in-memory-web-api';
import { Hero } from './hero';

@Injectable({
  providedIn: 'root',
})
export class InMemoryDataService implements InMemoryDbService {
  createDb() {
    const heroes = [
      { id: 12, name: 'Dr. Nice', power: 'Healing', level: 5, origin: 'Earth', isActive: true, description: 'A kind doctor with healing abilities' },
      { id: 13, name: 'Bombasto', power: 'Explosion', level: 7, origin: 'Mars', isActive: true, description: 'Creates powerful explosions' },
      { id: 14, name: 'Celeritas', power: 'Speed', level: 8, origin: 'Mercury', isActive: true, description: 'The fastest hero alive' },
      { id: 15, name: 'Magneta', power: 'Magnetism', level: 6, origin: 'Earth', isActive: false, description: 'Controls magnetic fields' },
      { id: 16, name: 'RubberMan', power: 'Elasticity', level: 4, origin: 'Earth', isActive: true, description: 'Can stretch his body to any shape' },
      { id: 17, name: 'Dynama', power: 'Energy', level: 7, origin: 'Jupiter', isActive: true, description: 'Manipulates energy forms' },
      { id: 18, name: 'Dr. IQ', power: 'Intelligence', level: 9, origin: 'Earth', isActive: true, description: 'Super genius with vast knowledge' },
      { id: 19, name: 'Magma', power: 'Fire', level: 6, origin: 'Venus', isActive: false, description: 'Controls fire and heat' },
      { id: 20, name: 'Tornado', power: 'Wind', level: 5, origin: 'Neptune', isActive: true, description: 'Creates powerful wind storms' }
    ];
    return {heroes};
  }

  // Overrides the genId method to ensure that a hero always has an id.
  // If the heroes array is empty,
  // the method below returns the initial number (11).
  // if the heroes array is not empty, the method below returns the highest
  // hero id + 1.
  genId(heroes: Hero[]): number {
    return heroes.length > 0 ? Math.max(...heroes.map(hero => hero.id)) + 1 : 11;
  }
}
