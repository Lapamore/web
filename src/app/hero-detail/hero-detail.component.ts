import { Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Observable, Subject, of } from 'rxjs';
import { takeUntil, tap, switchMap, catchError } from 'rxjs/operators';

import { Hero } from '../hero';
import { HeroService } from '../hero.service';

@Component({
  selector: 'app-hero-detail',
  templateUrl: './hero-detail.component.html',
  styleUrls: [ './hero-detail.component.css' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class HeroDetailComponent implements OnInit, OnDestroy {
  hero$!: Observable<Hero>;
  heroForm!: FormGroup;
  powers: string[] = ['Speed', 'Strength', 'Intelligence', 'Healing', 'Flight', 'Energy', 'Magnetism', 'Telekinesis', 'Invisibility', 'Fire', 'Ice'];
  origins: string[] = ['Earth', 'Mars', 'Jupiter', 'Venus', 'Mercury', 'Neptune', 'Unknown'];
  private destroy$ = new Subject<void>();

  constructor(
    private route: ActivatedRoute,
    private heroService: HeroService,
    private location: Location,
    private fb: FormBuilder
  ) {}

  ngOnInit(): void {
    this.createForm();
    this.getHero();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  createForm(): void {
    this.heroForm = this.fb.group({
      id: [{value: '', disabled: true}],
      name: ['', [Validators.required, Validators.minLength(3)]],
      power: ['', Validators.required],
      level: [1, [Validators.required, Validators.min(1), Validators.max(10)]],
      origin: ['', Validators.required],
      isActive: [true],
      description: ['', [Validators.required, Validators.maxLength(200)]]
    });
  }

  getHero(): void {
    const id = parseInt(this.route.snapshot.paramMap.get('id')!, 10);
    this.hero$ = this.heroService.getHero(id).pipe(
      tap(hero => {
        this.heroForm.patchValue({
          id: hero.id,
          name: hero.name,
          power: hero.power,
          level: hero.level,
          origin: hero.origin,
          isActive: hero.isActive,
          description: hero.description
        });
      }),
      catchError(error => {
        console.error('Error loading hero:', error);
        return of({} as Hero);
      })
    );
  }

  goBack(): void {
    this.location.back();
  }

  save(): void {
    if (this.heroForm.valid) {
      const formModel = this.heroForm.value;
      const heroId = parseInt(this.route.snapshot.paramMap.get('id')!, 10);
      
      const updatedHero: Hero = {
        id: heroId,
        name: formModel.name,
        power: formModel.power,
        level: formModel.level,
        origin: formModel.origin,
        isActive: formModel.isActive,
        description: formModel.description
      };
      
      this.heroService.updateHero(updatedHero)
        .pipe(takeUntil(this.destroy$))
        .subscribe(() => this.goBack());
    }
  }
}
