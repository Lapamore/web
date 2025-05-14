import { Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Observable, Subject } from 'rxjs';
import { takeUntil, switchMap, tap } from 'rxjs/operators';

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
  hero: Hero | undefined;
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
    this.heroService.getHero(id)
      .pipe(takeUntil(this.destroy$))
      .subscribe(hero => {
        this.hero = hero;
        this.heroForm.patchValue({
          id: hero.id,
          name: hero.name,
          power: hero.power,
          level: hero.level,
          origin: hero.origin,
          isActive: hero.isActive,
          description: hero.description
        });
      });
  }

  goBack(): void {
    this.location.back();
  }

  save(): void {
    if (this.heroForm.valid && this.hero) {
      const formModel = this.heroForm.value;
      
      this.hero.name = formModel.name;
      this.hero.power = formModel.power;
      this.hero.level = formModel.level;
      this.hero.origin = formModel.origin;
      this.hero.isActive = formModel.isActive;
      this.hero.description = formModel.description;
      
      this.heroService.updateHero(this.hero)
        .pipe(takeUntil(this.destroy$))
        .subscribe(() => this.goBack());
    }
  }
}
