import { Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { Observable, Subject, BehaviorSubject } from 'rxjs';
import { takeUntil, switchMap } from 'rxjs/operators';

import { Hero } from '../hero';
import { HeroService } from '../hero.service';

@Component({
  selector: 'app-heroes',
  templateUrl: './heroes.component.html',
  styleUrls: ['./heroes.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class HeroesComponent implements OnInit, OnDestroy {
  private heroesSubject = new BehaviorSubject<null>(null);
  heroes$: Observable<Hero[]>;
  private destroy$ = new Subject<void>();
  
  constructor(private heroService: HeroService) {
    // Создаем Observable, который будет обновляться при каждом вызове heroesSubject.next()
    this.heroes$ = this.heroesSubject.pipe(
      switchMap(() => this.heroService.getHeroes())
    );
  }

  ngOnInit(): void {
    // Инициируем первую загрузку
    this.loadHeroes();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.heroesSubject.complete();
  }

  // Вызываем этот метод для обновления списка героев
  loadHeroes(): void {
    this.heroesSubject.next(null);
  }

  add(name: string): void {
    name = name.trim();
    if (!name) { return; }
    
    // Создаем объект героя с обязательными полями
    const newHero: Partial<Hero> = { 
      name, 
      power: '', 
      level: 1,
      origin: '',
      isActive: true,
      description: ''
    };
    
    this.heroService.addHero(newHero as Hero)
      .pipe(takeUntil(this.destroy$))
      .subscribe(_ => {
        // Обновляем список героев после добавления
        this.loadHeroes();
      });
  }

  delete(hero: Hero): void {
    this.heroService.deleteHero(hero.id)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        // Обновляем список героев после удаления
        this.loadHeroes();
      });
  }
}
