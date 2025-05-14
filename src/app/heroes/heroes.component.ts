import { Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { Observable, Subject, Subscription } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { Hero } from '../hero';
import { HeroService } from '../hero.service';

@Component({
  selector: 'app-heroes',
  templateUrl: './heroes.component.html',
  styleUrls: ['./heroes.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class HeroesComponent implements OnInit, OnDestroy {
  heroes$!: Observable<Hero[]>;
  private destroy$ = new Subject<void>();
  
  constructor(private heroService: HeroService) { }

  ngOnInit(): void {
    this.getHeroes();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  getHeroes(): void {
    this.heroes$ = this.heroService.getHeroes();
  }

  add(name: string): void {
    name = name.trim();
    if (!name) { return; }
    this.heroService.addHero({ name } as Hero)
      .pipe(takeUntil(this.destroy$))
      .subscribe(hero => {
        // Обновляем список героев после добавления
        this.getHeroes();
      });
  }

  delete(hero: Hero): void {
    this.heroService.deleteHero(hero.id)
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        // Обновляем список героев после удаления
        this.getHeroes();
      });
  }
}
